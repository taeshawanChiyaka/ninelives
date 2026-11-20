# Contributing

1. Create a focused issue describing the correctness property or feature.
2. Add tests alongside implementation.
3. Run `pytest`, `ruff check .`, `ruff format --check .`, and `mypy src/ninelives`.
4. Keep runtime dependencies at zero.
5. Document platform and filesystem limitations honestly.

Contributions should preserve the rule that cache failures never silently change computed program results.
