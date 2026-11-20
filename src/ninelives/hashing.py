"""Deterministic, type-tagged fingerprints for supported Python values."""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import enum
import hashlib
import inspect
import math
import os
import struct
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any


class UnhashableArgumentError(TypeError):
    """Raised when a value cannot be fingerprinted deterministically."""


_HASHERS: dict[type[Any], Callable[[Any], bytes]] = {}


def register_hasher(value_type: type[Any], fn: Callable[[Any], bytes]) -> None:
    _HASHERS[value_type] = fn


def _frame(tag: bytes, payload: bytes) -> bytes:
    return tag + struct.pack(">Q", len(payload)) + payload


def _code_bytes(code: types.CodeType) -> bytes:
    consts = b"".join(_frame(b"C", _encode(c, set())) for c in code.co_consts)
    fields = (
        code.co_code,
        consts,
        repr(code.co_names).encode(),
        repr(code.co_varnames).encode(),
        repr(code.co_freevars).encode(),
        repr(code.co_cellvars).encode(),
    )
    return b"".join(_frame(b"F", x) for x in fields)


def _encode(value: Any, active: set[int]) -> bytes:
    if value is None:
        return _frame(b"N", b"")
    if isinstance(value, types.CodeType):
        return _frame(b"CODE", _code_bytes(value))
    if type(value) is bool:
        return _frame(b"B", b"1" if value else b"0")
    if type(value) is int:
        return _frame(b"I", str(value).encode())
    if type(value) is float:
        if math.isnan(value):
            raw = b"nan"
        elif value == 0.0:
            raw = b"-0" if math.copysign(1.0, value) < 0 else b"+0"
        else:
            raw = struct.pack(">d", value)
        return _frame(b"R", raw)
    if type(value) is str:
        return _frame(b"S", value.encode("utf-8"))
    if type(value) is bytes:
        return _frame(b"Y", value)
    if isinstance(value, (dt.datetime, dt.date)):
        return _frame(b"T", value.isoformat().encode())
    if isinstance(value, decimal.Decimal):
        return _frame(b"D", str(value.as_tuple()).encode())
    if isinstance(value, Path):
        return _frame(b"P", os.fsencode(value))
    if isinstance(value, enum.Enum):
        return _frame(
            b"E",
            _encode(value.__class__.__module__ + "." + value.__class__.__qualname__, active)
            + _encode(value.name, active),
        )
    oid = id(value)
    if oid in active:
        raise UnhashableArgumentError("Cyclic values are not supported by ninelives hashing.")
    custom = next((fn for typ, fn in _HASHERS.items() if isinstance(value, typ)), None)
    if custom is not None:
        result = custom(value)
        if not isinstance(result, bytes):
            raise UnhashableArgumentError("registered hashers must return bytes")
        return _frame(b"X", result)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        active.add(oid)
        try:
            payload = _encode(
                value.__class__.__module__ + "." + value.__class__.__qualname__, active
            )
            for field in dataclasses.fields(value):
                payload += _encode(field.name, active) + _encode(getattr(value, field.name), active)
            return _frame(b"A", payload)
        finally:
            active.remove(oid)
    if isinstance(value, (tuple, list, set, frozenset, dict)):
        active.add(oid)
        try:
            if isinstance(value, dict):
                pairs = sorted((_encode(k, active), _encode(v, active)) for k, v in value.items())
                payload = b"".join(_frame(b"K", k) + _frame(b"V", v) for k, v in pairs)
                tag = b"M"
            elif isinstance(value, (set, frozenset)):
                payload = b"".join(sorted(_encode(v, active) for v in value))
                tag = b"Z" if isinstance(value, set) else b"Q"
            else:
                payload = b"".join(_frame(b"V", _encode(v, active)) for v in value)
                tag = b"L" if isinstance(value, list) else b"U"
            return _frame(tag, payload)
        finally:
            active.remove(oid)
    # Optional array/dataframe/tensor support without importing their packages.
    module = type(value).__module__.split(".")[0]
    if module == "numpy" and hasattr(value, "dtype") and hasattr(value, "shape"):
        arr = value
        try:
            contiguous = arr.tobytes(order="C")
            return _frame(
                b"NP",
                _encode(str(arr.dtype), active) + _encode(tuple(arr.shape), active) + contiguous,
            )
        except Exception as exc:
            raise UnhashableArgumentError(f"Cannot fingerprint numpy value: {exc}") from exc
    if module == "pandas":
        try:
            if type(value).__name__ == "DataFrame":
                payload = _encode(tuple(map(str, value.columns)), active)
                payload += _encode(tuple(map(str, value.dtypes)), active)
                payload += _encode(value.index.to_list(), active)
                payload += _encode(value.to_numpy().tolist(), active)
            else:
                payload = (
                    _encode(str(value.dtype), active)
                    + _encode(value.index.to_list(), active)
                    + _encode(value.tolist(), active)
                )
            return _frame(b"PD", payload)
        except Exception as exc:
            raise UnhashableArgumentError(f"Cannot fingerprint pandas value: {exc}") from exc
    if module == "torch" and hasattr(value, "detach"):
        try:
            tensor = value.detach().cpu().contiguous()
            return _frame(
                b"TH",
                _encode(str(tensor.dtype), active)
                + _encode(tuple(tensor.shape), active)
                + tensor.numpy().tobytes(),
            )
        except Exception as exc:
            raise UnhashableArgumentError(f"Cannot fingerprint torch tensor: {exc}") from exc
    if callable(value):
        return _frame(b"CALL", callable_fingerprint(value))
    raise UnhashableArgumentError(
        f"Unsupported value of type {type(value).__module__}.{type(value).__qualname__}; "
        "use __ninelives_hash__ or nl.register_hasher(type, fn)."
    )


def canonical_bytes(value: Any) -> bytes:
    hook = getattr(value, "__ninelives_hash__", None)
    if callable(hook):
        result = hook()
        if not isinstance(result, bytes):
            raise UnhashableArgumentError("__ninelives_hash__ must return bytes")
        return _frame(b"HOOK", result)
    return _encode(value, set())


def fingerprint(value: Any) -> str:
    return hashlib.blake2b(canonical_bytes(value), digest_size=32).hexdigest()


def callable_fingerprint(fn: Any) -> bytes:
    target = fn
    if not (inspect.isfunction(target) or inspect.ismethod(target)) and callable(target):
        target = target.__call__
    code = getattr(target, "__code__", None)
    if code is None:
        raise UnhashableArgumentError(
            f"Callable {type(fn).__name__} has no fingerprintable Python code"
        )
    closure = []
    for cell in getattr(target, "__closure__", None) or ():
        try:
            closure.append(cell.cell_contents)
        except ValueError:
            closure.append("<empty-cell>")
    identity = f"{getattr(target, '__module__', '')}.{getattr(target, '__qualname__', '')}"
    payload = _encode(identity, set()) + _code_bytes(code)
    payload += _encode(getattr(target, "__defaults__", None), set())
    payload += _encode(getattr(target, "__kwdefaults__", None), set())
    payload += _encode(tuple(closure), set())
    payload += _encode(f"{sys.version_info.major}.{sys.version_info.minor}", set())
    return hashlib.blake2b(payload, digest_size=32).digest()
