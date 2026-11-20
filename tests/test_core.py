import asyncio
import contextlib
import importlib
import multiprocessing
import os
import runpy
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import SimpleNamespace

import pytest
from hypothesis import given
from hypothesis import strategies as st

import ninelives as nl
from ninelives.hashing import UnhashableArgumentError, canonical_bytes

cache_module = importlib.import_module("ninelives.cache")
checkpoint_module = importlib.import_module("ninelives.checkpoint")
cli_module = importlib.import_module("ninelives.cli")
locking_module = importlib.import_module("ninelives.locking")
storage_module = importlib.import_module("ninelives.storage")

_calls = 0
_calls_lock = threading.Lock()
_tracked_calls = []
_process_counter = None
_crash_value = b"old"


@dataclass
class _Record:
    value: int


class _Label(Enum):
    READY = 1


class _OtherLabel(Enum):
    READY = 1


class _CustomHash:
    def __init__(self, value):
        self.value = value

    def __ninelives_hash__(self):
        return str(self.value).encode()


class _Box:
    def __init__(self, value):
        self.value = value


def _same_body_one(value):
    return value


def _same_body_two(value):
    return value


def _counted(value):
    global _calls
    with _calls_lock:
        _calls += 1
    time.sleep(0.02)
    return value * 2


def _times_two(value):
    return value * 2


async def _async_times_two(value):
    await asyncio.sleep(0)
    return value * 2


async def _async_slow_times_two(value):
    await asyncio.sleep(0.03)
    return value * 2


async def _async_fail(value):
    _tracked_calls.append(value)
    raise RuntimeError(f"async failed {value}")


def _track(value):
    _tracked_calls.append(value)
    return value * 2


def _always_fail(value):
    _tracked_calls.append(value)
    raise RuntimeError(f"failed {value}")


def _fail_second(value):
    if value == 2:
        raise RuntimeError("failed second item")
    return value * 2


def _process_compute(value):
    with _process_counter.get_lock():
        _process_counter.value += 1
    time.sleep(0.1)
    return value + 5


def _call_cached_in_process(cache_dir, barrier, output):
    cached = nl.cache(cache_dir=cache_dir, lock=True)(_process_compute)
    barrier.wait()
    output.put(cached(7))


def _crash_compute(value):
    return _crash_value


def _crash_write_worker(cache_dir, ready):
    global _crash_value
    _crash_value = b"x" * (16 * 1024 * 1024)
    storage_module = importlib.import_module("ninelives.storage")
    original_fsync = storage_module.os.fsync

    def blocked_fsync(descriptor):
        original_fsync(descriptor)
        ready.send(True)
        time.sleep(60)

    storage_module.os.fsync = blocked_fsync
    nl.cache(cache_dir=cache_dir, refresh=True)(_crash_compute)(1)


def test_type_tags_and_order():
    assert nl.fingerprint(1) != nl.fingerprint(1.0)
    assert nl.fingerprint(1) != nl.fingerprint(True)
    assert nl.fingerprint({"a": 1, "b": 2}) == nl.fingerprint({"b": 2, "a": 1})
    assert nl.fingerprint({1, 2}) == nl.fingerprint({2, 1})


@given(st.dictionaries(st.text(), st.integers(), max_size=40))
def test_fingerprint_ignores_dictionary_insertion_order(mapping):
    assert nl.fingerprint(mapping) == nl.fingerprint(dict(reversed(list(mapping.items()))))


def test_set_fingerprint_is_stable_across_hash_seeds():
    code = (
        "from ninelives import fingerprint; print(fingerprint({'set': {'alpha', 'beta', 'gamma'}}))"
    )
    outputs = []
    for seed in ("1", "8675309"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.append(
            subprocess.check_output(
                [sys.executable, "-c", code], env=environment, text=True
            ).strip()
        )
    assert outputs[0] == outputs[1]


def test_fingerprint_supported_structured_values(tmp_path):
    assert nl.fingerprint(_Record(3)) == nl.fingerprint(_Record(3))
    assert nl.fingerprint(_Label.READY) != nl.fingerprint(_OtherLabel.READY)
    assert nl.fingerprint(date(2026, 1, 2)) != nl.fingerprint(Decimal("2026.0102"))
    assert nl.fingerprint(Path("relative/file")) == nl.fingerprint(Path("relative/file"))
    assert nl.fingerprint(_CustomHash(7)) == nl.fingerprint(_CustomHash(7))
    with pytest.raises(UnhashableArgumentError, match="must return bytes"):
        nl.fingerprint(type("BadHash", (), {"__ninelives_hash__": lambda self: "bad"})())
    from ninelives.hashing import callable_fingerprint

    assert callable_fingerprint(_same_body_one) != callable_fingerprint(_same_body_two)


def test_fingerprint_registered_and_optional_types(monkeypatch):
    hashing_module = importlib.import_module("ninelives.hashing")
    monkeypatch.setitem(hashing_module._HASHERS, tuple, lambda value: repr(value).encode())
    nl.register_hasher(tuple, lambda value: repr(value).encode())
    assert nl.fingerprint((1, 2)) == nl.fingerprint((1, 2))
    monkeypatch.setitem(hashing_module._HASHERS, _Record, lambda value: b"temporary")
    nl.register_hasher(_Record, lambda value: "invalid")
    with pytest.raises(UnhashableArgumentError, match="registered hashers"):
        nl.fingerprint(_Record(1))

    class NumpyArray:
        __module__ = "numpy"
        dtype = "int64"
        shape = (2,)

        def tobytes(self, order):
            assert order == "C"
            return b"array"

    assert nl.fingerprint(NumpyArray()) == nl.fingerprint(NumpyArray())

    class _Index:
        def to_list(self):
            return [0]

    class _NumpyValues:
        def tolist(self):
            return [[1]]

    DataFrame = type(
        "DataFrame",
        (),
        {
            "__module__": "pandas",
            "columns": ["value"],
            "dtypes": ["int64"],
            "index": _Index(),
            "to_numpy": lambda self: _NumpyValues(),
        },
    )
    assert nl.fingerprint(DataFrame()) == nl.fingerprint(DataFrame())

    Series = type(
        "Series",
        (),
        {
            "__module__": "pandas",
            "dtype": "int64",
            "index": _Index(),
            "tolist": lambda self: [1],
        },
    )
    assert nl.fingerprint(Series()) == nl.fingerprint(Series())

    class _Tensor:
        dtype = "float32"
        shape = (1,)

        def detach(self):
            return self

        def cpu(self):
            return self

        def contiguous(self):
            return self

        def numpy(self):
            return self

        def tobytes(self):
            return b"tensor"

    Tensor = type("Tensor", (_Tensor,), {"__module__": "torch"})
    assert nl.fingerprint(Tensor()) == nl.fingerprint(Tensor())

    BrokenArray = type(
        "BrokenArray",
        (),
        {
            "__module__": "numpy",
            "dtype": "int64",
            "shape": (1,),
            "tobytes": lambda self, order: (_ for _ in ()).throw(RuntimeError("broken")),
        },
    )
    with pytest.raises(UnhashableArgumentError, match="numpy value"):
        nl.fingerprint(BrokenArray())
    with pytest.raises(UnhashableArgumentError, match="Unsupported value"):
        nl.fingerprint(object())
    with pytest.raises(UnhashableArgumentError, match="no fingerprintable Python code"):
        hashing_module.callable_fingerprint(len)


def test_cycle_rejected():
    value = []
    value.append(value)
    with pytest.raises(UnhashableArgumentError):
        canonical_bytes(value)


def test_cache_hit_and_explain(tmp_path):
    calls = []

    @nl.cache(cache_dir=tmp_path)
    def f(x, scale=2):
        calls.append(x)
        return x * scale

    assert f(3) == f(3) == 6
    assert calls == [3]
    assert f.explain(3)["status"] == "HIT"
    assert f.explain(4)["status"] == "MISS"
    assert f.invalidate(3)
    assert f.explain(3)["status"] == "MISS"
    assert not f.invalidate(99)

    @nl.cache(cache_dir=tmp_path, ignore=["debug"])
    def ignored(value, debug="default"):
        return value

    assert ignored.key(1, debug="a") == ignored.key(1, debug="b")


def test_json_ttl_refresh_and_disabled_modes(tmp_path, monkeypatch):
    calls = []

    @nl.cache(cache_dir=tmp_path, serializer="json", ttl=0)
    def cached(value):
        calls.append(value)
        return {"value": value}

    assert cached(2) == {"value": 2}
    assert cached.explain(2)["status"] == "STALE"
    assert cached(2) == {"value": 2}
    assert calls == [2, 2]

    @nl.cache(cache_dir=tmp_path, refresh=True)
    def refreshed(value):
        calls.append(value)
        return value

    refreshed(5)
    refreshed(5)
    assert calls[-2:] == [5, 5]
    with nl.disabled():
        assert cached(3) == {"value": 3}
    monkeypatch.setenv("NINELIVES_DISABLE", "1")
    assert cached(4) == {"value": 4}
    assert cached.clear() >= 1


def test_unhashable_pickle_keeps_other_dependencies(tmp_path):
    config = tmp_path / "config.txt"
    config.write_text("first")

    @nl.cache(cache_dir=tmp_path, on_unhashable="pickle", watch_files=[str(config)])
    def read(box):
        return box.value

    assert read(_Box(9)) == 9
    config.write_text("second")
    assert read.explain(_Box(9))["status"] == "MISS"
    with pytest.raises(UnhashableArgumentError, match="Unsupported value"):
        nl.cache(cache_dir=tmp_path)(lambda value: value)(_Box(1))
    with pytest.raises(UnhashableArgumentError, match="ignore"):
        nl.cache(cache_dir=tmp_path, on_unhashable="ignore")(lambda value: value)(_Box(1))


def test_cache_strict_write_errors_and_cache_info(tmp_path, monkeypatch):
    @nl.cache(cache_dir=tmp_path, strict=True)
    def cached(value):
        return value

    monkeypatch.setattr(
        cache_module, "atomic_write", lambda *_: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError, match="disk"):
        cached(3)
    assert cached.cache_info()["misses"] == 1


def test_cache_configuration_and_fallback_paths(tmp_path, monkeypatch):
    real_os = cache_module.os
    with pytest.raises(ValueError, match="serializer"):
        nl.cache(serializer="invalid")
    with pytest.raises(ValueError, match="ttl"):
        nl.cache(ttl="tomorrow")
    with pytest.raises(ValueError, match="tags"):
        nl.cache(tags=[""])
    with pytest.raises(ValueError, match="on_unhashable"):
        nl.cache(on_unhashable="invalid")

    def generate():
        yield 1

    with pytest.raises(TypeError, match="Generator functions"):
        nl.cache(cache_dir=tmp_path)(generate)

    monkeypatch.setenv("NINELIVES_DIR", "~/ninelives-test")
    assert cache_module.default_cache_dir() == Path.home() / "ninelives-test"
    monkeypatch.delenv("NINELIVES_DIR")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    monkeypatch.setattr(cache_module.sys, "platform", "linux")
    monkeypatch.setattr(cache_module, "os", SimpleNamespace(name="posix", getenv=os.getenv))
    assert cache_module.default_cache_dir() == tmp_path / "xdg" / "ninelives"
    monkeypatch.delenv("XDG_CACHE_HOME")
    assert cache_module.default_cache_dir() == Path.home() / ".cache" / "ninelives"
    monkeypatch.setattr(cache_module.sys, "platform", "darwin")
    assert cache_module.default_cache_dir() == Path.home() / "Library" / "Caches" / "ninelives"
    monkeypatch.setattr(cache_module.sys, "platform", "linux")
    monkeypatch.setattr(cache_module, "os", SimpleNamespace(name="nt", getenv=os.getenv))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    assert cache_module.default_cache_dir() == tmp_path / "local" / "ninelives" / "Cache"
    monkeypatch.setattr(cache_module, "os", real_os)
    assert cache_module._package_version("nonexistent-ninelives-test-package") is None
    assert cache_module._ttl_seconds("2h") == 7200
    assert cache_module._ttl_seconds(2) == 2

    calls = []

    @nl.cache(cache_dir=tmp_path)
    def cached(value):
        calls.append(value)
        return value

    cached(1)
    monkeypatch.setenv("NINELIVES_REFRESH", "1")
    cached(1)
    assert calls == [1, 1]
    monkeypatch.delenv("NINELIVES_REFRESH")

    monkeypatch.setattr(cache_module, "default_cache_dir", lambda: tmp_path)
    assert nl.stats()["entries"] == 1
    nl.configure(compression=True)
    assert nl.clear_cache(function=cached) == 1
    with pytest.raises(ValueError, match="function-specific"):
        nl.clear_cache(function=cached, tag="tag")
    with pytest.raises(ValueError, match="cannot be combined"):
        nl.clear_cache(tag="tag", max_size=1, cache_dir=tmp_path)


def test_cache_lock_rechecks_key_after_acquiring_lock(tmp_path, monkeypatch):
    watched = tmp_path / "watched.txt"
    watched.write_text("before")
    changed = False

    @contextlib.contextmanager
    def update_during_lock(_):
        nonlocal changed
        if not changed:
            watched.write_text("after")
            changed = True
        yield

    monkeypatch.setattr(cache_module, "file_lock", update_during_lock)

    @nl.cache(cache_dir=tmp_path, lock=True, watch_files=[str(watched)])
    def read_watched(value):
        return value

    assert read_watched(1) == 1
    assert changed


def test_cache_returns_results_when_non_strict_storage_fails(tmp_path, monkeypatch):
    calls = []

    @nl.cache(cache_dir=tmp_path)
    def cached(value):
        calls.append(value)
        return value

    monkeypatch.setattr(
        cache_module,
        "atomic_write",
        lambda *_: (_ for _ in ()).throw(OSError("disk unavailable")),
    )
    assert cached(6) == 6
    assert cached.refresh(6) == 6
    assert calls == [6, 6]
    assert cached.cache_info()["misses"] == 2


def test_strict_refresh_raises_storage_errors(tmp_path, monkeypatch):
    @nl.cache(cache_dir=tmp_path, strict=True)
    def cached(value):
        return value

    monkeypatch.setattr(
        cache_module, "atomic_write", lambda *_: (_ for _ in ()).throw(OSError("disk"))
    )
    with pytest.raises(OSError, match="disk"):
        cached.refresh(1)


def test_strict_mode_surfaces_corrupt_entry_reads(tmp_path):
    @nl.cache(cache_dir=tmp_path, strict=True)
    def cached(value):
        return value

    cached(1)
    next(tmp_path.rglob("*.nlv")).write_bytes(b"bad")
    with pytest.raises(ValueError, match="truncated entry header"):
        cached(1)


def test_cache_clear_reports_unlink_failures(tmp_path, monkeypatch, caplog):
    @nl.cache(cache_dir=tmp_path)
    def cached(value):
        return value

    cached(1)
    entry = next(tmp_path.rglob("*.nlv"))
    original_unlink = Path.unlink

    def fail_entry_unlink(path, *args, **kwargs):
        if path == entry:
            raise OSError("permission denied")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_entry_unlink)
    assert cached.clear() == 0
    assert nl.clear_cache(cache_dir=tmp_path) == 0
    assert "Could not remove" in caplog.text


def test_tagged_clear_only_removes_matching_entries(tmp_path):
    tagged = nl.cache(cache_dir=tmp_path, tags=["weights"])(_times_two)
    untagged = nl.cache(cache_dir=tmp_path)(_track)
    tagged(2)
    untagged(3)
    assert nl.clear_cache(tag="weights", cache_dir=tmp_path) == 1
    assert len(list(tmp_path.rglob("*.nlv"))) == 1


def test_size_gc_removes_oldest_entries_until_under_limit(tmp_path):
    cached = nl.cache(cache_dir=tmp_path)(_times_two)
    cached(1)
    cached(2)
    assert nl.clear_cache(max_size=0, cache_dir=tmp_path) == 2
    assert not list(tmp_path.rglob("*.nlv"))
    assert nl.clear_cache(cache_dir=tmp_path) == 0
    with pytest.raises(ValueError, match="max_size"):
        nl.clear_cache(max_size=-1, cache_dir=tmp_path)


def test_cli_size_parser():
    assert cli_module._parse_size("1.5 MiB") == int(1.5 * 1024**2)
    with pytest.raises(cli_module.argparse.ArgumentTypeError):
        cli_module._parse_size("-1GB")


def test_cli_list_inspect_gc_and_clear(tmp_path, monkeypatch, capsys):
    @nl.cache(cache_dir=tmp_path)
    def cached(value):
        return value + 1

    cached(4)
    monkeypatch.setattr(cli_module, "default_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["ninelives", "ls"])
    cli_module.main()
    assert cached.key(4) in capsys.readouterr().out

    monkeypatch.setattr(sys, "argv", ["ninelives", "inspect", cached.key(4)])
    cli_module.main()
    assert '"payload_bytes"' in capsys.readouterr().out

    monkeypatch.setattr(sys, "argv", ["ninelives", "gc", "--older-than", "0s", "--max-size", "0B"])
    cli_module.main()
    assert "removed 1 entries" in capsys.readouterr().out

    cached(4)
    monkeypatch.setattr(sys, "argv", ["ninelives", "clear"])
    cli_module.main()
    assert "removed 1 entries" in capsys.readouterr().out


def test_cli_verify_reports_corrupt_entries(tmp_path, monkeypatch, capsys):
    @nl.cache(cache_dir=tmp_path)
    def cached(value):
        return value

    cached(1)
    next(tmp_path.rglob("*.nlv")).write_bytes(b"broken")
    monkeypatch.setattr(cli_module, "default_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["ninelives", "verify"])
    with pytest.raises(SystemExit, match="1"):
        cli_module.main()
    assert "INVALID" in capsys.readouterr().out


def test_cli_inspect_missing_and_verify_success(tmp_path, monkeypatch, capsys):
    @nl.cache(cache_dir=tmp_path)
    def cached(value):
        return value

    cached(1)
    monkeypatch.setattr(cli_module, "default_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["ninelives", "inspect", "missing"])
    with pytest.raises(SystemExit, match="2"):
        cli_module.main()
    monkeypatch.setattr(sys, "argv", ["ninelives", "verify"])
    cli_module.main()
    assert "verified 1/1 entries" in capsys.readouterr().out


def test_module_entry_point_runs_cli(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli_module, "default_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(sys, "argv", ["ninelives", "ls"])
    runpy.run_module("ninelives.__main__", run_name="__main__")


def test_corruption_recomputes(tmp_path):
    @nl.cache(cache_dir=tmp_path)
    def f(x):
        return x + 9

    assert f(1) == 10
    next(tmp_path.rglob("*.nlv")).write_bytes(b"broken")
    assert f(1) == 10


def test_resumable_map(tmp_path):
    first = nl.resumable_map(_times_two, range(12), name="unit", chunk_size=4, cache_dir=tmp_path)
    second = nl.resumable_map(_times_two, range(12), name="unit", chunk_size=4, cache_dir=tmp_path)
    assert first == second == [value * 2 for value in range(12)]


def test_cache_lock_is_single_flight_for_threads(tmp_path):
    global _calls
    _calls = 0
    cached = nl.cache(cache_dir=tmp_path, lock=True)(_counted)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(cached, [3] * 8)) == [6] * 8
    assert _calls == 1


def test_cache_lock_is_single_flight_across_processes(tmp_path):
    global _process_counter
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("fork-based process lock test requires fork support")
    context = multiprocessing.get_context("fork")
    _process_counter = context.Value("i", 0)
    barrier = context.Barrier(2)
    output = context.Queue()
    processes = [
        context.Process(target=_call_cached_in_process, args=(tmp_path, barrier, output))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    assert [output.get(timeout=2) for _ in processes] == [12, 12]
    assert _process_counter.value == 1


def test_sigkill_during_cache_write_preserves_previous_entry(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("SIGKILL crash injection requires fork support")
    global _crash_value
    _crash_value = b"old"
    cached = nl.cache(cache_dir=tmp_path)(_crash_compute)
    assert cached(1) == b"old"
    context = multiprocessing.get_context("fork")
    ready, sender = context.Pipe(duplex=False)
    process = context.Process(target=_crash_write_worker, args=(tmp_path, sender))
    process.start()
    sender.close()
    assert ready.recv() is True
    os.kill(process.pid, signal.SIGKILL)
    process.join(timeout=10)
    assert process.exitcode == -signal.SIGKILL
    assert cached(1) == b"old"


def test_resumable_map_invalidates_only_changed_items(tmp_path):
    _tracked_calls.clear()
    first = nl.resumable_map(_track, [1, 2, 3], name="stale", cache_dir=tmp_path)
    _tracked_calls.clear()
    second = nl.resumable_map(_track, [1, 20, 3], name="stale", cache_dir=tmp_path)
    assert first == [2, 4, 6]
    assert second == [2, 40, 6]
    assert _tracked_calls == [20]
    assert len(list((tmp_path / "runs").rglob("*.seg"))) == 1


def test_resumable_map_can_reject_stale_inputs(tmp_path):
    nl.resumable_map(_times_two, [1, 2], name="stale-error", cache_dir=tmp_path)
    with pytest.raises(ValueError, match="inputs changed"):
        nl.resumable_map(
            _times_two, [1, 20], name="stale-error", on_stale="error", cache_dir=tmp_path
        )


def test_resumable_map_verify_none_explicitly_reuses_results(tmp_path):
    nl.resumable_map(
        _times_two, [1, 2], name="verify-none", verify_inputs="none", cache_dir=tmp_path
    )
    results = nl.resumable_map(
        _times_two, [10, 20], name="verify-none", verify_inputs="none", cache_dir=tmp_path
    )
    assert results == [2, 4]


def test_resumable_map_error_policies_and_validation(tmp_path):
    _tracked_calls.clear()
    recorded = nl.resumable_map(
        _always_fail,
        [2],
        name="record-failures",
        retries=2,
        backoff=0,
        on_item_error="record",
        cache_dir=tmp_path,
    )
    assert recorded == [{"__ninelives_error__": "RuntimeError('failed 2')"}]
    assert _tracked_calls == [2, 2, 2]

    skipped = nl.resumable_map(
        _always_fail,
        [3],
        name="skip-failures",
        retries=0,
        backoff=0,
        on_item_error="skip",
        cache_dir=tmp_path,
    )
    assert skipped == []

    iterator = nl.resumable_map(
        _times_two, [1, 2], name="iter-results", collect=False, cache_dir=tmp_path
    )
    assert list(iterator) == [(0, 2), (1, 4)]
    with pytest.raises(ValueError, match="workers"):
        nl.resumable_map(_times_two, [], name="bad-workers", workers=0, cache_dir=tmp_path)
    with pytest.raises(ValueError, match="executor"):
        nl.resumable_map(
            _times_two, [], name="bad-executor", executor="invalid", cache_dir=tmp_path
        )
    with pytest.raises(ValueError, match="commit_every"):
        nl.resumable_map(_times_two, [], name="bad-commit", commit_every=-1, cache_dir=tmp_path)


def test_resumable_map_graceful_flag_controls_failure_commit(tmp_path):
    with pytest.raises(RuntimeError, match="second item"):
        nl.resumable_map(
            _fail_second,
            [1, 2],
            name="not-graceful",
            chunk_size=4,
            retries=0,
            graceful=False,
            cache_dir=tmp_path,
        )
    assert not list((tmp_path / "runs").rglob("*.seg"))

    with pytest.raises(RuntimeError, match="second item"):
        nl.resumable_map(
            _fail_second,
            [1, 2],
            name="graceful",
            chunk_size=4,
            retries=0,
            graceful=True,
            cache_dir=tmp_path,
        )
    assert list((tmp_path / "runs").rglob("*.seg"))


def test_threaded_resumable_map_preserves_order(tmp_path):
    actual = nl.resumable_map(
        _times_two, range(20), name="threads", workers=4, chunk_size=5, cache_dir=tmp_path
    )
    assert actual == [value * 2 for value in range(20)]


def test_process_resumable_map_preserves_order(tmp_path):
    if "COV_CORE_SOURCE" in os.environ:
        pytest.skip("ProcessPoolExecutor is tested separately without coverage instrumentation")
    actual = nl.resumable_map(
        _times_two,
        range(12),
        name="processes",
        workers=2,
        executor="process",
        chunk_size=4,
        cache_dir=tmp_path,
    )
    assert actual == [value * 2 for value in range(12)]


def test_async_resumable_map_is_concurrent_and_ordered(tmp_path):
    actual = asyncio.run(
        nl.async_resumable_map(
            _async_times_two,
            range(12),
            name="async",
            workers=3,
            chunk_size=6,
            cache_dir=tmp_path,
        )
    )
    assert actual == [value * 2 for value in range(12)]


def test_async_resumable_map_error_policies_and_validation(tmp_path):
    _tracked_calls.clear()

    async def execute():
        result = await nl.async_resumable_map(
            _async_fail,
            [4],
            name="async-errors",
            retries=1,
            backoff=0,
            on_item_error="record",
            cache_dir=tmp_path,
        )
        assert result == [{"__ninelives_error__": "RuntimeError('async failed 4')"}]
        with pytest.raises(ValueError, match="workers"):
            await nl.async_resumable_map(
                _async_times_two, [], name="async-bad-workers", workers=0, cache_dir=tmp_path
            )
        with pytest.raises(ValueError, match="commit_every"):
            await nl.async_resumable_map(
                _async_times_two, [], name="async-bad-commit", commit_every=-1, cache_dir=tmp_path
            )

    asyncio.run(execute())
    assert _tracked_calls == [4, 4]


def test_async_resumable_map_cancellation_commits_completed_work(tmp_path):
    async def cancel_after_first_batch():
        task = asyncio.create_task(
            nl.async_resumable_map(
                _async_slow_times_two,
                range(8),
                name="cancel-async",
                workers=2,
                chunk_size=2,
                cache_dir=tmp_path,
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_after_first_batch())
    assert list((tmp_path / "runs").rglob("*.seg"))
    resumed = asyncio.run(
        nl.async_resumable_map(
            _async_slow_times_two,
            range(8),
            name="cancel-async",
            workers=2,
            chunk_size=2,
            cache_dir=tmp_path,
        )
    )
    assert resumed == [value * 2 for value in range(8)]


def test_hmac_is_checked_before_deserialization(tmp_path, monkeypatch):
    @nl.cache(cache_dir=tmp_path, hmac_key=b"k" * 32)
    def signed(value):
        return value + 1

    assert signed(4) == 5
    assert signed.explain(4)["status"] == "HIT"
    monkeypatch.setattr(cache_module, "_deserialize", lambda *_: pytest.fail("deserialized"))

    @nl.cache(cache_dir=tmp_path, hmac_key=b"x" * 32)
    def signed(value):
        return value + 1

    assert signed(4) == 5


def test_checkpoint_hmac_is_checked_before_unpickling(tmp_path, monkeypatch):
    nl.resumable_map(_times_two, [1, 2], name="signed-run", hmac_key=b"k" * 32, cache_dir=tmp_path)
    monkeypatch.setattr(
        checkpoint_module.pickle, "loads", lambda *_: pytest.fail("unpickled unauthenticated data")
    )
    result = nl.resumable_map(
        _times_two, [1, 2], name="signed-run", hmac_key=b"x" * 32, cache_dir=tmp_path
    )
    assert result == [2, 4]


def test_hmac_key_must_be_strong_enough():
    with pytest.raises(ValueError, match="at least 32 bytes"):
        nl.cache(hmac_key=b"short")


def test_windows_file_lock_backend(monkeypatch, tmp_path):
    lock_modes = []

    def lock_file(descriptor, mode, length):
        lock_modes.append(mode)
        if len(lock_modes) == 1:
            raise OSError(13, "lock is held")

    fake_msvcrt = SimpleNamespace(locking=lock_file, LK_NBLCK=1, LK_UNLCK=2)
    monkeypatch.setattr(locking_module.os, "name", "nt")
    monkeypatch.setitem(sys.modules, "msvcrt", fake_msvcrt)
    with locking_module.file_lock(tmp_path / "windows.lock"):
        assert lock_modes == [1, 1]
    assert lock_modes == [1, 1, 2]


def test_storage_roundtrip_hmac_and_corruption(tmp_path):
    path = tmp_path / "entry"
    payload = b"payload"
    metadata = storage_module.signed_metadata(payload, {"key": "value"}, b"k" * 32)
    storage_module.atomic_write(path, payload, metadata)
    stored_meta, stored_payload = storage_module.read_entry(path)
    storage_module.verify_entry_hmac(stored_meta, stored_payload, b"k" * 32)
    with pytest.raises(ValueError, match="HMAC verification failed"):
        storage_module.verify_entry_hmac(stored_meta, stored_payload, b"x" * 32)
    with pytest.raises(ValueError, match="HMAC verification failed"):
        storage_module.verify_entry_hmac({**stored_meta, "hmac": 12}, stored_payload, b"k" * 32)
    path.write_bytes(b"bad")
    with pytest.raises(ValueError, match="truncated"):
        storage_module.read_entry(path)


def test_storage_checks_payload_length_and_checksum(tmp_path):
    path = tmp_path / "entry"
    storage_module.atomic_write(path, b"payload", {})
    encoded = bytearray(path.read_bytes())
    encoded[-1] ^= 1
    path.write_bytes(encoded)
    with pytest.raises(ValueError, match="checksum"):
        storage_module.read_entry(path)

    storage_module.atomic_write(path, b"payload", {})
    encoded = path.read_bytes()
    _, _, metadata_length = storage_module.HEADER.unpack(encoded[: storage_module.HEADER.size])
    start = storage_module.HEADER.size
    metadata = storage_module.json.loads(encoded[start : start + metadata_length])
    metadata["payload_length"] += 1
    encoded_metadata = storage_module.json.dumps(
        metadata, sort_keys=True, separators=(",", ":")
    ).encode()
    path.write_bytes(
        storage_module.HEADER.pack(
            storage_module.MAGIC, storage_module.FORMAT_VERSION, len(encoded_metadata)
        )
        + encoded_metadata
        + encoded[start + metadata_length :]
    )
    with pytest.raises(ValueError, match="payload length"):
        storage_module.read_entry(path)


def test_storage_rejects_bad_headers_hmac_and_cleans_failed_writes(tmp_path, monkeypatch):
    path = tmp_path / "entry"
    path.write_bytes(storage_module.HEADER.pack(b"BAD!", 1, 0))
    with pytest.raises(ValueError, match="magic"):
        storage_module.read_entry(path)

    path.write_bytes(storage_module.HEADER.pack(storage_module.MAGIC, 99, 0))
    with pytest.raises(ValueError, match="unsupported"):
        storage_module.read_entry(path)

    path.write_bytes(storage_module.HEADER.pack(storage_module.MAGIC, 1, 3) + b"{}")
    with pytest.raises(ValueError, match="truncated metadata"):
        storage_module.read_entry(path)

    with pytest.raises(ValueError, match="not HMAC-signed"):
        storage_module.verify_entry_hmac({}, b"payload", b"k" * 32)
    signed = storage_module.signed_metadata(b"payload", {}, b"k" * 32)
    with pytest.raises(ValueError, match="requires an HMAC key"):
        storage_module.verify_entry_hmac(signed, b"payload", None)

    def fail_replace(*_):
        raise OSError("replace failed")

    monkeypatch.setattr(storage_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        storage_module.atomic_write(path, b"payload", {})
    assert not list(tmp_path.glob("*.tmp"))
    assert not list(tmp_path.glob(".*.tmp"))
