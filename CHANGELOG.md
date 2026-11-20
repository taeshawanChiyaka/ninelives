# Changelog

All notable changes are documented here.

## [0.1.0] - 2026-11-20
- Initial alpha implementation of deterministic fingerprints, atomic cache entries, explain, checkpoint segments, resumable mapping, CLI, and project documentation.
- Added per-key process locks for cached calls and per-run locks for resumable execution.
- Added bounded thread/process workers, async mapping, per-input stale-result detection, and optional HMAC verification before cache or checkpoint deserialization.
- Added tagged and size-limited garbage collection.
- Expanded property, concurrency, and POSIX SIGKILL tests; broader platform/crash testing remains.
