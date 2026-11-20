"""Portable advisory file locks for local cache coordination."""

from __future__ import annotations

import errno
import os
import threading
import time
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

_local_locks: weakref.WeakValueDictionary[str, threading.RLock] = weakref.WeakValueDictionary()
_local_locks_guard = threading.Lock()


@contextmanager
def file_lock(path: Path) -> Iterator[None]:
    """Lock a local file across threads and processes until the context exits."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = str(path.resolve())
    with _local_locks_guard:
        local_lock = _local_locks.setdefault(key, threading.RLock())
    with local_lock, path.open("a+b") as stream:
        if os.name == "nt":
            import msvcrt

            stream.seek(0)
            if stream.read(1) == b"":
                stream.write(b"\0")
                stream.flush()
            lock_file = getattr(msvcrt, "locking")  # noqa: B009
            lock_exclusive = getattr(msvcrt, "LK_NBLCK")  # noqa: B009
            lock_release = getattr(msvcrt, "LK_UNLCK")  # noqa: B009
            while True:
                try:
                    stream.seek(0)
                    lock_file(stream.fileno(), lock_exclusive, 1)
                    break
                except OSError as exc:
                    if (
                        exc.errno not in {errno.EACCES, errno.EDEADLK}
                        and getattr(exc, "winerror", None) != 33
                    ):
                        raise
                    time.sleep(0.05)
            try:
                yield
            finally:
                stream.seek(0)
                lock_file(stream.fileno(), lock_release, 1)
        else:
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
