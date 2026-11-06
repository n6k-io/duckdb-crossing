# crossing

## Invariants

1. **It is DuckDB.** Same SQL and same answers, wherever the work runs.
2. **The only addition is the seam.** The plan, the pipelines and how one part waits for another are all DuckDB's.
3. **Crossing decides what runs where.** It keeps DuckDB's semantics while doing it.
4. **A source is a transport.** It says what it has and what it can run, then moves plans and rows.
5. **Never a silently wrong answer.** If crossing can't do something correctly, it fails fast, and a person writing SQL can predict that.
6. **Simple, logical and consistent.** One idea that applies to everything.

## About

DuckDB library that pushes plan fragments to a foreign source. `IMPLEMENTING.md` describes the source interface.

## Build and test

Needs cmake and a C++17 compiler.

```
make test
```

Builds DuckDB from `duckdb/` into `build/duckdb`, the test binaries into `build/crossing`, runs them, then runs the `test/contract/` compile-fail cases via ctest. `TEST_ARGS="[tag]"` filters Catch tags.

`local.mk` (gitignored) may set `DUCKDB_SOURCE_DIR` and `DUCKDB_BUILD_DIR`.

## Format

```
make format-check
make format-fix
```

`clang-format` is pinned in `pyproject.toml`; `uv sync` installs it into `.venv`.
