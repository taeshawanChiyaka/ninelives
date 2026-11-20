"""Segment-based resumable iteration."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import pickle
import random
import threading
import time
import uuid
from collections.abc import Iterable, Iterator
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .hashing import fingerprint
from .locking import file_lock
from .storage import atomic_write, read_entry, signed_metadata, verify_entry_hmac

log = logging.getLogger("ninelives")


def _execute_item(
    fn: Any, item: Any, retries: int, backoff: float, on_item_error: str
) -> tuple[bool, Any]:
    for attempt in range(retries + 1):
        try:
            return True, fn(item)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            if attempt < retries:
                time.sleep(backoff * (2**attempt) * (0.75 + random.random() * 0.5))
                continue
            if on_item_error == "record":
                return True, {"__ninelives_error__": repr(exc)}
            if on_item_error == "skip":
                return False, None
            raise
    raise AssertionError("unreachable retry loop")


async def _async_resumable_map_unlocked(
    fn: Any,
    items: Iterable[Any],
    *,
    name: str,
    chunk_size: int,
    commit_every: float,
    workers: int,
    retries: int,
    backoff: float,
    on_item_error: str,
    on_stale: str,
    verify_inputs: str,
    collect: bool,
    graceful: bool,
    cache_dir: str | os.PathLike[str] | None,
    hmac_key: bytes | None,
    cancel_event: threading.Event,
) -> Any:
    if workers < 1:
        raise ValueError("workers must be >= 1")
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    if commit_every < 0:
        raise ValueError("commit_every must be >= 0")
    if retries < 0 or backoff < 0:
        raise ValueError("retries and backoff must be >= 0")
    if on_item_error not in {"raise", "skip", "record"}:
        raise ValueError("invalid on_item_error")
    if on_stale not in {"invalidate", "error"}:
        raise ValueError("invalid on_stale")
    if verify_inputs not in {"full", "sample", "none"}:
        raise ValueError("invalid verify_inputs")
    sequence = list(items)
    if verify_inputs == "full":
        input_digests = {index: fingerprint(item) for index, item in enumerate(sequence)}
        verification = "full"
    elif verify_inputs == "sample":
        input_digests = {}
        verification = fingerprint(sequence[:10])
    else:
        input_digests = {}
        verification = "none"
    run_fp = fingerprint({"function": fn, "length": len(sequence), "verification": verification})
    result_map: dict[int, Any] = {}
    with Checkpoint(
        name,
        fingerprint=run_fp,
        cache_dir=cache_dir,
        commit_every=commit_every,
        input_digests=input_digests,
        hmac_key=hmac_key,
    ) as checkpoint:
        stale = checkpoint.stale_indexes()
        if stale and on_stale == "error":
            raise ValueError(f"Checkpoint inputs changed at indexes: {sorted(stale)}")
        result_map.update(checkpoint.iter_results())
        pending = list(checkpoint.pending(enumerate(sequence)))

        async def compute(index: int, item: Any) -> tuple[bool, Any]:
            for attempt in range(retries + 1):
                if cancel_event.is_set():
                    raise asyncio.CancelledError
                try:
                    return True, await fn(item)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if attempt < retries:
                        await asyncio.sleep(backoff * (2**attempt) * (0.75 + random.random() * 0.5))
                        continue
                    if on_item_error == "record":
                        return True, {"__ninelives_error__": repr(exc)}
                    if on_item_error == "skip":
                        return False, None
                    raise
            raise AssertionError("unreachable retry loop")

        for offset in range(0, len(pending), chunk_size):
            chunk = pending[offset : offset + chunk_size]
            for batch_start in range(0, len(chunk), workers):
                batch = chunk[batch_start : batch_start + workers]
                tasks = [asyncio.create_task(compute(index, item)) for index, item in batch]
                try:
                    outcomes = await asyncio.gather(*tasks)
                    for (index, _), (should_save, value) in zip(batch, outcomes):
                        if should_save:
                            checkpoint.save(index, value, input_digests.get(index))
                            result_map[index] = value
                except BaseException:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    if graceful:
                        checkpoint.commit()
                    raise
            checkpoint.commit()
        if stale:
            checkpoint.reconcile()
    ordered = [result_map[index] for index in sorted(result_map)]
    if collect:
        return ordered
    return Checkpoint(
        name,
        fingerprint=run_fp,
        cache_dir=cache_dir,
        input_digests=input_digests,
        hmac_key=hmac_key,
    ).iter_results()


def _run_async_resumable_map(
    fn: Any,
    items: Iterable[Any],
    *,
    name: str,
    cancel_event: threading.Event,
    options: dict[str, Any],
) -> Any:
    from .cache import default_cache_dir

    cache_dir = options["cache_dir"]
    base = Path(cache_dir) if cache_dir else default_cache_dir()
    run_key = hashlib.blake2b(name.encode(), digest_size=12).hexdigest()
    with file_lock(base / "runs" / f"{run_key}.lock"):
        return asyncio.run(
            _async_resumable_map_unlocked(
                fn, items, name=name, cancel_event=cancel_event, **options
            )
        )


class Checkpoint:
    """Low-level checkpoint. Committed segment files are the source of truth."""

    def __init__(
        self,
        name: str,
        *,
        fingerprint: str,
        cache_dir: str | os.PathLike[str] | None = None,
        commit_every: float = 30.0,
        input_digests: dict[int, str] | None = None,
        hmac_key: bytes | None = None,
    ):
        from .cache import default_cache_dir

        base = Path(cache_dir) if cache_dir else default_cache_dir()
        self.directory = base / "runs" / hashlib.blake2b(name.encode(), digest_size=12).hexdigest()
        self.run_fingerprint = fingerprint
        self.commit_every = commit_every
        self.input_digests = input_digests or {}
        if hmac_key is not None and (not isinstance(hmac_key, bytes) or len(hmac_key) < 32):
            raise ValueError("hmac_key must be bytes containing at least 32 bytes")
        self.hmac_key = hmac_key
        self.pending_values: dict[int, Any] = {}
        self.pending_input_digests: dict[int, str] = {}
        self.last_commit = time.monotonic()
        self._commit_lock = threading.RLock()

    def __enter__(self) -> Checkpoint:  # noqa: PYI034
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        return self

    def __exit__(self, *exc: object) -> None:
        if not exc or exc[0] is None:
            self.commit()

    def _read_segments(
        self, *, include_stale: bool = False
    ) -> list[tuple[int, int, dict[int, Any], dict[str, Any]]]:
        found = []
        for path in self.directory.glob("*.seg"):
            try:
                meta, payload = read_entry(path)
                if meta.get("run_fingerprint") != self.run_fingerprint:
                    continue
                verify_entry_hmac(meta, payload, self.hmac_key)
                values = pickle.loads(payload)
                if not isinstance(values, dict) or not all(type(index) is int for index in values):
                    raise TypeError("segment payload is not an integer-index mapping")
                if fingerprint(sorted(values.items())) != meta.get("input_digest"):
                    raise ValueError("segment result digest mismatch")
                raw_digests = meta.get("input_digests", {})
                if not isinstance(raw_digests, dict):
                    raise TypeError("segment input digests are invalid")
                input_digests: dict[str, str] = {}
                for index, digest in raw_digests.items():
                    if not isinstance(index, str) or not isinstance(digest, str):
                        raise TypeError("segment input digests are invalid")
                    input_digests[index] = digest
                if not include_stale and self.input_digests:
                    values = {
                        index: value
                        for index, value in values.items()
                        if input_digests.get(str(index)) == self.input_digests.get(index)
                    }
                found.append((int(meta["start"]), int(meta["end"]), values, meta))
            except Exception as error:  # noqa: BLE001
                log.warning("Ignoring invalid checkpoint segment %s: %s", path, error)
        return sorted(found, key=lambda item: (item[0], item[1]))

    def stale_indexes(self) -> set[int]:
        if not self.input_digests:
            return set()
        stale: set[int] = set()
        for _, _, values, meta in self._read_segments(include_stale=True):
            stored = meta.get("input_digests", {})
            if isinstance(stored, dict):
                stale.update(
                    index
                    for index in values
                    if stored.get(str(index)) != self.input_digests.get(index)
                )
        return stale

    def pending(self, items: Iterable[tuple[int, Any]]) -> Iterator[tuple[int, Any]]:
        covered = {index for _, _, segment, _ in self._read_segments() for index in segment}
        for index, item in items:
            if index not in covered:
                yield index, item

    def save(self, index: int, result: Any, input_digest: str | None = None) -> None:
        with self._commit_lock:
            self.pending_values[index] = result
            if input_digest is not None:
                self.pending_input_digests[index] = input_digest
            if (
                len(self.pending_values) >= 128
                or time.monotonic() - self.last_commit >= self.commit_every
            ):
                self.commit()

    def commit(self) -> None:
        with self._commit_lock:
            if not self.pending_values:
                return
            indexes = sorted(self.pending_values)
            groups: list[list[int]] = []
            for index in indexes:
                if not groups or index != groups[-1][-1] + 1:
                    groups.append([index])
                else:
                    groups[-1].append(index)
            for group in groups:
                values = {index: self.pending_values[index] for index in group}
                metadata = {
                    "run_fingerprint": self.run_fingerprint,
                    "start": group[0],
                    "end": group[-1] + 1,
                    "input_digest": fingerprint(sorted(values.items())),
                    "input_digests": {
                        str(index): self.pending_input_digests[index]
                        for index in group
                        if index in self.pending_input_digests
                    },
                }
                payload = pickle.dumps(values, protocol=5)
                filename = f"{group[0]:020d}-{group[-1] + 1:020d}-{uuid.uuid4().hex}.seg"
                atomic_write(
                    self.directory / filename,
                    payload,
                    signed_metadata(payload, metadata, self.hmac_key),
                )
                for index in group:
                    self.pending_values.pop(index, None)
                    self.pending_input_digests.pop(index, None)
            self.last_commit = time.monotonic()

    def iter_results(self) -> Iterator[tuple[int, Any]]:
        results: dict[int, Any] = {}
        for _, _, segment, _ in self._read_segments():
            results.update(segment)
        yield from sorted(results.items())

    def reconcile(self) -> None:
        """Replace old/stale segments with one committed snapshot of valid results."""
        self.commit()
        values = dict(self.iter_results())
        replacement: Path | None = None
        if values:
            indexes = sorted(values)
            metadata = {
                "run_fingerprint": self.run_fingerprint,
                "start": indexes[0],
                "end": indexes[-1] + 1,
                "input_digest": fingerprint(sorted(values.items())),
                "input_digests": {
                    str(index): self.input_digests[index]
                    for index in indexes
                    if index in self.input_digests
                },
            }
            payload = pickle.dumps(values, protocol=5)
            replacement = self.directory / (
                f"{indexes[0]:020d}-{indexes[-1] + 1:020d}-{uuid.uuid4().hex}.seg"
            )
            atomic_write(replacement, payload, signed_metadata(payload, metadata, self.hmac_key))
        for path in self.directory.glob("*.seg"):
            if path == replacement:
                continue
            try:
                path.unlink()
            except OSError as exc:
                log.warning("Could not remove stale checkpoint segment %s: %s", path, exc)


def _resumable_map_unlocked(
    fn: Any,
    items: Iterable[Any],
    *,
    name: str,
    chunk_size: int = 256,
    commit_every: float = 30.0,
    workers: int = 1,
    executor: str = "thread",
    retries: int = 3,
    backoff: float = 2.0,
    on_item_error: str = "raise",
    on_stale: str = "invalidate",
    verify_inputs: str = "full",
    collect: bool = True,
    graceful: bool = True,
    cache_dir: str | os.PathLike[str] | None = None,
    hmac_key: bytes | None = None,
) -> Any:
    if workers < 1:
        raise ValueError("workers must be >= 1")
    if executor not in {"thread", "process"}:
        raise ValueError("executor must be 'thread' or 'process'")
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")
    if commit_every < 0:
        raise ValueError("commit_every must be >= 0")
    if retries < 0 or backoff < 0:
        raise ValueError("retries and backoff must be >= 0")
    if on_item_error not in {"raise", "skip", "record"}:
        raise ValueError("invalid on_item_error")
    if on_stale not in {"invalidate", "error"}:
        raise ValueError("invalid on_stale")
    if verify_inputs not in {"full", "sample", "none"}:
        raise ValueError("invalid verify_inputs")
    sequence = list(items)
    if verify_inputs == "full":
        input_digests = {index: fingerprint(item) for index, item in enumerate(sequence)}
        verification = "full"
    elif verify_inputs == "sample":
        input_digests = {}
        verification = fingerprint(sequence[:10])
    else:
        input_digests = {}
        verification = "none"
    run_fp = fingerprint({"function": fn, "length": len(sequence), "verification": verification})
    result_map: dict[int, Any] = {}
    with Checkpoint(
        name,
        fingerprint=run_fp,
        cache_dir=cache_dir,
        commit_every=commit_every,
        input_digests=input_digests,
        hmac_key=hmac_key,
    ) as checkpoint:
        stale = checkpoint.stale_indexes()
        if stale and on_stale == "error":
            raise ValueError(f"Checkpoint inputs changed at indexes: {sorted(stale)}")
        if stale:
            log.info("Invalidating stale checkpoint results at indexes: %s", sorted(stale))
        result_map.update(checkpoint.iter_results())
        pending = list(checkpoint.pending(enumerate(sequence)))

        for offset in range(0, len(pending), chunk_size):
            chunk = pending[offset : offset + chunk_size]
            try:
                if workers == 1:
                    outcomes = (
                        (index, _execute_item(fn, item, retries, backoff, on_item_error))
                        for index, item in chunk
                    )
                    for index, (should_save, value) in outcomes:
                        if should_save:
                            checkpoint.save(index, value, input_digests.get(index))
                            result_map[index] = value
                else:
                    pool: Executor
                    if executor == "thread":
                        pool = ThreadPoolExecutor(max_workers=workers)
                    else:
                        pool = ProcessPoolExecutor(max_workers=workers)
                    with pool:
                        futures = {
                            pool.submit(
                                _execute_item, fn, item, retries, backoff, on_item_error
                            ): index
                            for index, item in chunk
                        }
                        for future in as_completed(futures):
                            index = futures[future]
                            should_save, value = future.result()
                            if should_save:
                                checkpoint.save(index, value, input_digests.get(index))
                                result_map[index] = value
            except BaseException:
                if graceful:
                    checkpoint.commit()
                raise
            checkpoint.commit()
        if stale:
            checkpoint.reconcile()
    ordered = [result_map[index] for index in sorted(result_map)]
    if collect:
        return ordered
    return Checkpoint(
        name,
        fingerprint=run_fp,
        cache_dir=cache_dir,
        input_digests=input_digests,
        hmac_key=hmac_key,
    ).iter_results()


def resumable_map(
    fn: Any,
    items: Iterable[Any],
    *,
    name: str,
    chunk_size: int = 256,
    commit_every: float = 30.0,
    workers: int = 1,
    executor: str = "thread",
    retries: int = 3,
    backoff: float = 2.0,
    on_item_error: str = "raise",
    on_stale: str = "invalidate",
    verify_inputs: str = "full",
    collect: bool = True,
    graceful: bool = True,
    cache_dir: str | os.PathLike[str] | None = None,
    hmac_key: bytes | None = None,
) -> Any:
    """Map over inputs with durable chunks and a cross-process per-run lock."""
    from .cache import default_cache_dir

    base = Path(cache_dir) if cache_dir else default_cache_dir()
    run_key = hashlib.blake2b(name.encode(), digest_size=12).hexdigest()
    with file_lock(base / "runs" / f"{run_key}.lock"):
        return _resumable_map_unlocked(
            fn,
            items,
            name=name,
            chunk_size=chunk_size,
            commit_every=commit_every,
            workers=workers,
            executor=executor,
            retries=retries,
            backoff=backoff,
            on_item_error=on_item_error,
            on_stale=on_stale,
            verify_inputs=verify_inputs,
            collect=collect,
            graceful=graceful,
            cache_dir=cache_dir,
            hmac_key=hmac_key,
        )


async def async_resumable_map(
    fn: Any,
    items: Iterable[Any],
    *,
    name: str,
    chunk_size: int = 256,
    commit_every: float = 30.0,
    workers: int = 32,
    retries: int = 3,
    backoff: float = 2.0,
    on_item_error: str = "raise",
    on_stale: str = "invalidate",
    verify_inputs: str = "full",
    collect: bool = True,
    graceful: bool = True,
    cache_dir: str | os.PathLike[str] | None = None,
    hmac_key: bytes | None = None,
) -> Any:
    """Run an async mapper concurrently with ordered durable commits."""
    cancel_event = threading.Event()
    options = {
        "chunk_size": chunk_size,
        "commit_every": commit_every,
        "workers": workers,
        "retries": retries,
        "backoff": backoff,
        "on_item_error": on_item_error,
        "on_stale": on_stale,
        "verify_inputs": verify_inputs,
        "collect": collect,
        "graceful": graceful,
        "cache_dir": cache_dir,
        "hmac_key": hmac_key,
    }
    task = asyncio.create_task(
        asyncio.to_thread(
            _run_async_resumable_map,
            fn,
            items,
            name=name,
            cancel_event=cancel_event,
            options=options,
        )
    )
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        cancel_event.set()
        try:
            await task
        except asyncio.CancelledError:
            pass
        raise
