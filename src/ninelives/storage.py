"""Atomic file storage with checksummed, versioned entries."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import struct
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

log = logging.getLogger("ninelives")
MAGIC = b"NLV1"
FORMAT_VERSION = 1
HEADER = struct.Struct(">4sIQ")  # magic, format, metadata length


def signed_metadata(payload: bytes, metadata: dict[str, Any], key: bytes | None) -> dict[str, Any]:
    result = dict(metadata)
    if key is not None:
        message = (
            json.dumps(result, sort_keys=True, separators=(",", ":")).encode() + b"\0" + payload
        )
        result["hmac"] = hmac.new(key, message, hashlib.sha256).hexdigest()
    return result


def verify_entry_hmac(meta: dict[str, Any], payload: bytes, key: bytes | None) -> None:
    signature = meta.get("hmac")
    if signature is None:
        if key is not None:
            raise ValueError("cache entry is not HMAC-signed")
        return
    if key is None:
        raise ValueError("cache entry requires an HMAC key")
    unsigned_meta = {
        name: value
        for name, value in meta.items()
        if name not in {"hmac", "checksum", "payload_length"}
    }
    message = (
        json.dumps(unsigned_meta, sort_keys=True, separators=(",", ":")).encode() + b"\0" + payload
    )
    expected = hmac.new(key, message, hashlib.sha256).hexdigest()
    if not isinstance(signature, str) or not hmac.compare_digest(signature, expected):
        raise ValueError("cache entry HMAC verification failed")


def atomic_write(path: Path, payload: bytes, metadata: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    meta = dict(metadata)
    meta["checksum"] = hashlib.blake2b(payload, digest_size=32).hexdigest()
    meta["payload_length"] = len(payload)
    meta_bytes = json.dumps(meta, sort_keys=True, separators=(",", ":")).encode()
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.", suffix=".tmp", dir=path.parent
    )
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(HEADER.pack(MAGIC, FORMAT_VERSION, len(meta_bytes)))
            stream.write(meta_bytes)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        for attempt in range(5):
            try:
                os.replace(tmp, path)
                break
            except PermissionError:
                if os.name != "nt" or attempt == 4:
                    raise
                time.sleep(0.02 * (attempt + 1))
        if os.name != "nt":
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def read_entry(path: Path) -> tuple[dict[str, Any], bytes]:
    with path.open("rb") as stream:
        raw = stream.read(HEADER.size)
        if len(raw) != HEADER.size:
            raise ValueError("truncated entry header")
        magic, version, meta_len = HEADER.unpack(raw)
        if magic != MAGIC:
            raise ValueError("invalid cache magic")
        if version != FORMAT_VERSION:
            raise ValueError(f"unsupported cache format {version}")
        meta_raw = stream.read(meta_len)
        if len(meta_raw) != meta_len:
            raise ValueError("truncated metadata")
        meta = json.loads(meta_raw)
        payload = stream.read()
    if len(payload) != meta.get("payload_length"):
        raise ValueError("payload length mismatch")
    checksum = hashlib.blake2b(payload, digest_size=32).hexdigest()
    if checksum != meta.get("checksum"):
        raise ValueError("payload checksum mismatch")
    return meta, payload


def safe_cache_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "posix":
        st = path.stat()
        getuid = getattr(os, "getuid", None)
        if getuid is None:
            raise RuntimeError("POSIX cache ownership checks require os.getuid")
        if st.st_uid != getuid():
            raise PermissionError(f"Cache directory is not owned by current user: {path}")
        path.chmod(0o700)
    return path
