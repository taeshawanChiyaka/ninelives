"""Function-level memoization with inspectable cache misses."""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import logging
import os
import pickle
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ParamSpec, Protocol, TypeVar, cast

from .hashing import UnhashableArgumentError, callable_fingerprint, fingerprint
from .locking import file_lock
from .storage import atomic_write, read_entry, safe_cache_dir, signed_metadata, verify_entry_hmac

P = ParamSpec("P")
R = TypeVar("R")
log = logging.getLogger("ninelives")
_FORMAT = "ninelives-1"


class CachedFunction(Protocol[P, R]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...

    explain: Callable[..., dict[str, Any]]
    key: Callable[..., str]
    refresh: Callable[..., R]
    invalidate: Callable[..., bool]
    clear: Callable[[], int]
    cache_info: Callable[[], dict[str, Any]]


def default_cache_dir() -> Path:
    env = os.getenv("NINELIVES_DIR")
    if env:
        return Path(env).expanduser()
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "ninelives"
    if os.name == "nt":
        return Path(os.getenv("LOCALAPPDATA", Path.home())) / "ninelives" / "Cache"
    return Path(os.getenv("XDG_CACHE_HOME", Path.home() / ".cache")) / "ninelives"


def _ttl_seconds(value: str | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        return float(value[:-1]) * units[value[-1].lower()]
    except (ValueError, KeyError, IndexError) as exc:
        raise ValueError(f"Invalid ttl: {value}") from exc


def cache(
    *,
    cache_dir: str | os.PathLike[str] | None = None,
    version: str = "v1",
    deps: list[Any] | None = None,
    watch_files: list[str] | None = None,
    env: list[str] | None = None,
    track_packages: list[str] | None = None,
    ignore: list[str] | None = None,
    ttl: str | float | None = None,
    serializer: str = "pickle",
    refresh: bool = False,
    strict: bool = False,
    on_unhashable: str = "error",
    lock: bool = False,
    hmac_key: bytes | None = None,
    tags: list[str] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Memoize deterministic function calls. Cache failures recompute unless strict=True."""
    if serializer not in {"pickle", "json"}:
        raise ValueError("serializer must be 'pickle' or 'json'")
    if on_unhashable not in {"error", "pickle", "ignore"}:
        raise ValueError("invalid on_unhashable")
    if hmac_key is not None and (not isinstance(hmac_key, bytes) or len(hmac_key) < 32):
        raise ValueError("hmac_key must be bytes containing at least 32 bytes")
    if tags is not None and any(not isinstance(tag, str) or not tag for tag in tags):
        raise ValueError("tags must contain non-empty strings")
    entry_tags = sorted(set(tags or []))
    ttl_s = _ttl_seconds(ttl)

    def decorate(fn: Callable[P, R]) -> Callable[P, R]:
        if inspect.isgeneratorfunction(fn):
            raise TypeError("Generator functions are unsupported; use nl.resumable_map.")
        root = safe_cache_dir(
            Path(cache_dir).expanduser() if cache_dir is not None else default_cache_dir()
        )
        func_id = hashlib.blake2b(
            (fn.__module__ + "." + fn.__qualname__).encode(), digest_size=12
        ).hexdigest()
        func_dir = root / func_id
        stats = {"hits": 0, "misses": 0, "time_saved": 0.0}
        code_fp = callable_fingerprint(fn).hex()

        def with_key_lock(
            args: tuple[Any, ...], kwargs: dict[str, Any], call: Callable[[], R]
        ) -> R:
            if not lock:
                return call()
            while True:
                key, _ = make_key(args, kwargs)
                with file_lock(func_dir / ".locks" / f"{key}.lock"):
                    if make_key(args, kwargs)[0] != key:
                        continue
                    return call()

        @functools.wraps(fn)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if os.getenv("NINELIVES_DISABLE") == "1" or getattr(_STATE, "disabled", False):
                return fn(*args, **kwargs)
            if refresh or os.getenv("NINELIVES_REFRESH") == "1":
                return with_key_lock(
                    args,
                    kwargs,
                    lambda: _compute_and_store(
                        fn,
                        args,
                        kwargs,
                        func_dir,
                        make_key,
                        serializer,
                        strict,
                        stats,
                        hmac_key,
                        entry_tags,
                    ),
                )
            return with_key_lock(
                args,
                kwargs,
                lambda: _get_or_compute(
                    fn,
                    args,
                    kwargs,
                    func_dir,
                    make_key,
                    serializer,
                    strict,
                    stats,
                    ttl_s,
                    hmac_key,
                    entry_tags,
                ),
            )

        def make_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[str, dict[str, str]]:
            bound = inspect.signature(fn).bind(*args, **kwargs)
            bound.apply_defaults()
            for name in ignore or []:
                bound.arguments.pop(name, None)
            parts: dict[str, str] = {"code": code_fp, "version": fingerprint(version)}
            try:
                parts["args"] = fingerprint(bound.arguments)
            except UnhashableArgumentError:
                if on_unhashable == "pickle":
                    parts["args"] = hashlib.blake2b(
                        pickle.dumps(bound.arguments, protocol=5), digest_size=32
                    ).hexdigest()
                elif on_unhashable == "ignore":
                    raise UnhashableArgumentError(
                        "on_unhashable='ignore' is not safe for persistent cache keys; provide a hasher or use pickle."
                    )
                else:
                    raise
            parts["deps"] = fingerprint(deps or [])
            parts["files"] = fingerprint(
                [
                    (p, Path(p).read_bytes() if Path(p).exists() else None)
                    for p in (watch_files or [])
                ]
            )
            parts["env"] = fingerprint({k: os.getenv(k) for k in (env or [])})
            parts["packages"] = fingerprint(
                {k: _package_version(k) for k in (track_packages or [])}
            )
            return fingerprint(parts), parts

        cached = cast(CachedFunction[P, R], wrapped)
        cached.explain = lambda *a, **kw: _explain(func_dir, make_key, a, kw, ttl_s, hmac_key)
        cached.key = lambda *a, **kw: make_key(a, kw)[0]
        cached.refresh = lambda *a, **kw: with_key_lock(
            a,
            kw,
            lambda: _compute_and_store(
                fn, a, kw, func_dir, make_key, serializer, strict, stats, hmac_key, entry_tags
            ),
        )
        cached.invalidate = lambda *a, **kw: _invalidate(func_dir, make_key, a, kw)
        cached.clear = lambda: _clear(func_dir)
        cached.cache_info = lambda: dict(stats)
        return cached

    return decorate


def _package_version(name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as get_version

    try:
        return get_version(name)
    except PackageNotFoundError:
        return None


def _entry(directory: Path, key: str) -> Path:
    return directory / f"{key}.nlv"


def _serialize(value: Any, serializer: str) -> bytes:
    if serializer == "json":
        return json.dumps(value, sort_keys=True).encode()
    return pickle.dumps(value, protocol=5)


def _deserialize(data: bytes, serializer: str) -> Any:
    return json.loads(data) if serializer == "json" else pickle.loads(data)


def _get_or_compute(
    fn: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    directory: Path,
    make_key: Callable[..., Any],
    serializer: str,
    strict: bool,
    stats: dict[str, Any],
    ttl_s: float | None,
    hmac_key: bytes | None,
    tags: list[str],
) -> R:
    started = time.perf_counter()
    try:
        key, parts = make_key(args, kwargs)
        path = _entry(directory, key)
        if path.exists():
            try:
                meta, payload = read_entry(path)
                if meta.get("parts") != parts:
                    raise ValueError("fingerprint components differ")
                if ttl_s is not None and time.time() - meta["created"] > ttl_s:
                    raise ValueError("TTL expired")
                verify_entry_hmac(meta, payload, hmac_key)
                value = _deserialize(payload, serializer)
                stats["hits"] += 1
                stats["time_saved"] += time.perf_counter() - started
                try:
                    os.utime(path, None)
                except OSError as exc:
                    log.warning("Could not update cache access time for %s: %s", path, exc)
                return cast(R, value)
            except Exception as exc:
                if strict:
                    raise
                log.warning("ninelives cache entry %s is unusable; recomputing: %s", key, exc)
                try:
                    path.rename(path.with_suffix(".corrupt"))
                except OSError as move_exc:
                    log.warning("Could not quarantine cache entry %s: %s", key, move_exc)
        stats["misses"] += 1
        value = fn(*args, **kwargs)
        try:
            payload = _serialize(value, serializer)
            metadata = signed_metadata(
                payload,
                {
                    "format": _FORMAT,
                    "created": time.time(),
                    "parts": parts,
                    "function": fn.__qualname__,
                    "tags": tags,
                },
                hmac_key,
            )
            atomic_write(path, payload, metadata)
        except Exception as exc:
            log.warning("ninelives could not write cache entry; returning computed result: %s", exc)
            if strict:
                raise
        return value
    except Exception:
        if strict:
            raise
        # Hash ambiguity is a programming error, not a cache I/O failure.
        raise


def _compute_and_store(
    fn: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    directory: Path,
    make_key: Callable[..., Any],
    serializer: str,
    strict: bool,
    stats: dict[str, Any],
    hmac_key: bytes | None,
    tags: list[str],
) -> R:
    key, parts = make_key(args, kwargs)
    value = fn(*args, **kwargs)
    try:
        payload = _serialize(value, serializer)
        metadata = signed_metadata(
            payload,
            {
                "format": _FORMAT,
                "created": time.time(),
                "parts": parts,
                "function": fn.__qualname__,
                "tags": tags,
            },
            hmac_key,
        )
        atomic_write(_entry(directory, key), payload, metadata)
    except Exception as exc:
        log.warning("ninelives could not write refreshed entry: %s", exc)
        if strict:
            raise
    stats["misses"] += 1
    return value


def _explain(
    directory: Path,
    make_key: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    ttl_s: float | None,
    hmac_key: bytes | None,
) -> dict[str, Any]:
    key, parts = make_key(args, kwargs)
    path = _entry(directory, key)
    if not path.exists():
        return {
            "status": "MISS",
            "key": key,
            "reasons": ["no entry exists for this key"],
            "components": parts,
        }
    try:
        meta, payload = read_entry(path)
        reasons = []
        if meta.get("parts") != parts:
            reasons.append("fingerprint components changed")
        if ttl_s is not None and time.time() - meta["created"] > ttl_s:
            reasons.append("TTL expired")
        try:
            verify_entry_hmac(meta, payload, hmac_key)
        except ValueError as exc:
            reasons.append(str(exc))
        return {
            "status": "STALE" if reasons else "HIT",
            "key": key,
            "reasons": reasons,
            "components": parts,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "STALE",
            "key": key,
            "reasons": [f"entry invalid: {exc}"],
            "components": parts,
        }


def _invalidate(
    directory: Path, make_key: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> bool:
    key, _ = make_key(args, kwargs)
    path = _entry(directory, key)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def _clear(directory: Path) -> int:
    count = 0
    if directory.exists():
        for path in directory.glob("*.nlv"):
            try:
                path.unlink()
                count += 1
            except OSError as exc:
                log.warning("Could not remove %s: %s", path, exc)
    return count


class _State:
    disabled = False


_STATE = _State()


class _Disabled:
    def __enter__(self) -> None:
        self.old = _STATE.disabled
        _STATE.disabled = True

    def __exit__(self, *exc: object) -> None:
        _STATE.disabled = self.old


def disabled() -> _Disabled:
    return _Disabled()


def clear_cache(
    function: Any = None,
    older_than: str | None = None,
    tag: str | None = None,
    max_size: int | None = None,
    cache_dir: str | os.PathLike[str] | None = None,
) -> int:
    """Clear old, tagged, or over-limit cache entries and checkpoint segments."""
    if function is not None:
        if (
            older_than is not None
            or tag is not None
            or max_size is not None
            or cache_dir is not None
        ):
            raise ValueError("function-specific clearing cannot be combined with global filters")
        return cast(int, function.clear())
    if max_size is not None and max_size < 0:
        raise ValueError("max_size must be >= 0")
    if tag is not None and max_size is not None:
        raise ValueError("tag and max_size cannot be combined")
    root = Path(cache_dir).expanduser() if cache_dir is not None else default_cache_dir()
    if not root.exists():
        return 0
    age_seconds = _ttl_seconds(older_than) if older_than is not None else None
    entries = [path for pattern in ("*.nlv", "*.seg") for path in root.rglob(pattern)]
    records: list[tuple[Path, int, float, bool]] = []
    for path in entries:
        try:
            stat = path.stat()
            eligible = True
            if tag is not None:
                eligible = False
                if path.suffix == ".nlv":
                    metadata, _ = read_entry(path)
                    stored_tags = metadata.get("tags", [])
                    eligible = isinstance(stored_tags, list) and tag in stored_tags
            records.append((path, stat.st_size, stat.st_mtime, eligible))
        except (OSError, ValueError, TypeError) as exc:
            log.warning("Could not inspect cache entry %s: %s", path, exc)

    count = 0
    now = time.time()
    retained: list[tuple[Path, int, float, bool]] = []
    total_size = 0
    for path, size, modified, eligible in records:
        expired = age_seconds is not None and now - modified > age_seconds and eligible
        clear_all = older_than is None and tag is None and max_size is None
        clear_tagged = tag is not None and eligible
        if expired or clear_all or clear_tagged:
            try:
                path.unlink()
                count += 1
            except OSError as exc:
                log.warning("Could not clear %s: %s", path, exc)
                retained.append((path, size, modified, eligible))
                total_size += size
        else:
            retained.append((path, size, modified, eligible))
            total_size += size

    if max_size is not None and total_size > max_size:
        for path, size, _, eligible in sorted(retained, key=lambda row: row[2]):
            if total_size <= max_size:
                break
            if not eligible:
                continue
            try:
                path.unlink()
                count += 1
                total_size -= size
            except OSError as exc:
                log.warning("Could not clear %s: %s", path, exc)
    return count


def stats() -> dict[str, int]:
    root = default_cache_dir()
    return {"entries": len(list(root.rglob("*.nlv"))) if root.exists() else 0}


def configure(**kwargs: Any) -> None:
    """Reserved for future global configuration; currently validates no global mutable settings."""
    if kwargs:
        log.info(
            "Per-decorator configuration is preferred; ignored global options: %s", sorted(kwargs)
        )
