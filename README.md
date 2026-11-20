# ninelives
Crash-resilient, resumable Python work with careful cache invalidation.

**Your job can die eight times.**

`ninelives` is a Python library for long-running computations that need durable checkpoints, resumable iteration, and explainable cache invalidation. It is not intended to replace `joblib.Memory`, `diskcache`, or `cachier`; its focus is chunked commits, safer invalidation, and `explain()`.

## 60-second quickstart

```bash
pip install "git+https://github.com/taeshawanChiyaka/ninelives.git"
```

```python
import ninelives as nl


@nl.cache(version="feature-v1", watch_files=["config.yaml"], env=["EMBED_MODEL"])
def build_features(document, window=7):
    return expensive_feature_extraction(document, window)


features = build_features("document")
print(build_features.explain("document"))
```

For resumable work:

```python
results = nl.resumable_map(
    embed,
    documents,
    name="embed-corpus-v1",
    chunk_size=256,
    commit_every=30,
    retries=3,
)
```

The run name is a stable identifier. The input sequence and function fingerprint form the run identity. A changed identity starts a new run directory/fingerprint rather than reusing incompatible segments.

## Core guarantees and limitations

- Entry files are written to a temporary file in the destination directory, flushed, fsynced, and atomically replaced; POSIX parent directories are fsynced.
- Payload checksums and format versions are checked before deserialization.
- Corrupt entries are logged and recomputed in normal cache mode.
- `strict=True` makes cache read/write failures visible instead of silently recomputing.
- Hashing is type-tagged and deterministic for supported built-in values; unsupported values raise `UnhashableArgumentError`.
- `pickle` is the default serializer. **Never load a cache directory writable by an untrusted party:** pickle deserialization can execute arbitrary code.
- Atomic replace is not a universal durability guarantee on NFS, SMB, FUSE, removable media, or every filesystem/controller. Power-loss guarantees depend on filesystem and hardware behavior.
- Cache invalidation includes function bytecode/constants/defaults/closures, arguments, explicit dependencies, watched file bytes, selected environment variables, selected package versions, Python minor version, and the manual version tag. It does not automatically detect arbitrary files opened inside the function, network state, wall-clock time, randomness, database contents, or transitive dependencies beyond explicitly supplied `deps`.
- A cache is an optimization. If storage is unavailable, normal mode logs and returns the computed result; `strict=True` raises on cache storage failures.

## API

- `nl.cache(...)`: function memoization with `.explain()`, `.refresh()`, `.invalidate()`, `.clear()`, `.key()`, `.cache_info()`.
- `nl.resumable_map(...)`: resumable mapping with chunk commits, selective input invalidation,
  retry policies, and bounded thread or process workers. Concurrent processes using the same run
  name are serialized by a local file lock. Process-mode functions and inputs must be picklable.
- `nl.async_resumable_map(...)`: async-function equivalent with bounded concurrent tasks; cancellation
  waits for active items to finish so their commits remain consistent.
- `nl.Checkpoint(...)`: low-level indexed checkpoint API.
- `nl.fingerprint(value)`, `nl.register_hasher(type, fn)`.
- `nl.clear_cache()`, `nl.stats()`, `nl.configure()`, `nl.disabled()`.

Pass `lock=True` to `nl.cache(...)` for per-key single-flight execution across local threads and
processes. Pass a secret `hmac_key` of at least 32 bytes to `nl.cache(...)` or `nl.resumable_map(...)`
to authenticate entries; keep the key outside the cache directory and use the same key when
reading. Authentication is verified before deserializing the payload. Without a key, pickle
remains trusted-local only.

Use `tags=["model-weights"]` on a cache decorator to group entries for `nl.clear_cache(tag=...)`.
`nl.clear_cache(max_size=...)` and `ninelives gc --max-size 1GB` evict least-recently-used entries
until the on-disk cache is within the requested byte limit.

## Comparison

| Capability | ninelives | joblib.Memory | diskcache | cachier |
|---|---|---|---|---|
| Function memoization | Yes | Yes | Yes | Yes |
| Explain miss components | Yes, v0.1 component report | Not its primary focus | Not its primary focus | Not its primary focus |
| Durable resumable indexed segments | Yes, thread/process/async | No core resumable-map API | Can be composed | No core resumable-map API |
| Zero runtime dependencies | Yes | No (joblib/numpy ecosystem) | No | No |
| Remote backend | Deferred | Varies by integration | Supported patterns | Varies |

## Development

```bash
python -m pip install -e ".[dev]"
pytest --cov=ninelives --cov-branch
ruff check .
ruff format --check .
mypy src/ninelives
python -m build
twine check dist/*
```

## CLI

```bash
ninelives ls
ninelives inspect <key>
ninelives clear --older-than 30d
ninelives gc --older-than 30d --max-size 1GB
ninelives verify
```

## Security

See [SECURITY.md](SECURITY.md). Pickle caches are trusted-local artifacts unless signed with a separately stored HMAC key; signing does not authorize an untrusted shared cache.
