# How invalidation works

The cache key incorporates normalized function bytecode and constants, defaults, keyword defaults, closure values, Python minor version, arguments after `ignore`, manual `version`, explicit `deps`, watched file contents, selected environment variables, and selected installed package versions.

The implementation does not recursively discover arbitrary global helper functions or all mutable globals. Include relevant helpers/configuration in `deps`, watch relevant files, pass configuration as arguments, or increment `version`. Files opened internally by a function, randomness, time, network responses, external databases, and hidden process state are not automatically tracked.

`explain()` is a dry-run: it computes the candidate key and checks the corresponding entry without writing or deleting data. It reports HIT, MISS, or STALE and the current component fingerprints. For resumable maps, `verify_inputs="full"` stores a digest per input and invalidates only changed item results; `on_stale="error"` raises instead. `"sample"` checks only the first ten items, and `"none"` opts out of input-change detection.
