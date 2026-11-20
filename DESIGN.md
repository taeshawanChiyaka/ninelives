# ninelives Design (v0.1)

## On-disk layout
Default root is XDG cache on Linux, `~/Library/Caches/ninelives` on macOS, and `%LOCALAPPDATA%/ninelives/Cache` on Windows. Function entries are grouped under a digest of the qualified function name and named only by the hexadecimal call key. Resumable runs are under `runs/<digest(run name)>/` and contain immutable `.seg` files. Segment files are authoritative; no manifest is required.

## Entry format
An entry starts with `NLV1`, a 32-bit format version, and a 64-bit metadata byte length. UTF-8 JSON metadata follows, then the payload. Metadata includes payload length and BLAKE2b checksum. Payload serialization is pickle protocol 5 or JSON. Unknown versions, truncation, and checksum mismatches are invalid.

## Fingerprinting
The call key is BLAKE2b-256 over a canonical, length-prefixed, type-tagged encoding of component fingerprints. Built-in scalar/container values, dates, decimals, paths, enums, dataclasses, callables, and opportunistically detected numpy/pandas/torch values are supported. Dict keys and set members are sorted by encoded bytes. `bool`, `int`, and `float` are distinct; NaN and signed zero have explicit encodings. Cycles and unsupported values fail explicitly. Function fingerprints include qualified identity, bytecode, constants, names, defaults, keyword defaults, closure values, and Python minor version.

## Invalidation inputs
Keys include function code, arguments after ignored parameters are removed, manual version, explicit `deps`, watched file bytes, selected environment variables, and selected installed package versions. The v0.1 implementation does not recursively fingerprint referenced global helpers or global literals; pass those through `deps` or bump `version`. It cannot infer arbitrary I/O or nondeterminism.

## Atomicity and failure model
Writes use a unique same-directory temporary file, flush+fsync, atomic replace, then parent-directory fsync on POSIX. Readers validate header and checksum before decoding. Cache storage errors are logged and recomputed in normal mode; `strict=True` surfaces storage errors. This does not promise power-loss durability beyond the guarantees of the underlying filesystem and hardware. NFS/network filesystem semantics are not guaranteed.

## Resumable segments
A segment records a half-open index range, run fingerprint, result digest, per-index input digests (when full verification is enabled), and result mapping. A segment is immutable once written. Resume schedules indices not present in valid segments; changed inputs invalidate only their corresponding saved results. `on_stale="error"` rejects the run instead. `sample` validates only the first ten inputs, while `none` disables input-change detection.

## Concurrency
Atomic replacement protects readers from partial cache files. `cache(..., lock=True)` takes a per-key local process lock for single-flight computation. `resumable_map` takes a per-run process lock and supports bounded thread or process executors; process-mode callables and inputs must be picklable. Calls to the same run name from separate processes are serialized. `async_resumable_map` runs an async mapper in a dedicated event loop with bounded concurrent tasks; cancellation is cooperative between items. Do not share active run directories across hosts or network filesystems.

## Garbage collection
Global garbage collection considers cache entries and checkpoint segments. Age filters remove entries whose access time exceeds the requested age. Size caps count all entries and evict oldest eligible files first. Decorator tags apply to function-cache entries; checkpoint segments are not tag-addressable.

## Signed entries
`cache(..., hmac_key=...)` and `resumable_map(..., hmac_key=...)` require at least 32 secret bytes and authenticate metadata and payload using HMAC-SHA256. Signed entries are verified before deserialization. A configured key is required to read signed entries, and unsigned entries are rejected when a key is configured. HMAC does not make an attacker-controlled cache directory safe if the signing key is also exposed to that attacker.

## API decisions
Correctness takes priority over silently accepting unsupported values. Hash ambiguity raises `UnhashableArgumentError`. Cache payload serialization defaults to pickle for broad Python support, with the associated trust boundary. `json` is opt-in and only works for JSON-serializable values.
