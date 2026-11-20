# Roadmap

## v0.2
- Signal-handler graceful shutdown that restores prior handlers.
- Global helper/global literal dependency graph fingerprinting with configurable depth.
- Rich component-level `explain()` diffs and persisted last-seen records.
- Optional NumPy `.npy` / Parquet serializers.
- Broader property-based tests and randomized crash-injection matrix on Linux/macOS/Windows (current SIGKILL test covers POSIX).

## Deferred by design
- S3/Redis and other remote backends.
- DAG/workflow orchestration.
- Web UI.
- Caching generator streams.
- Memmap optimizations.
