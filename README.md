# crossing

DuckDB library that pushes plan fragments to a foreign source. `IMPLEMENTING.md` describes the source interface.

## Build and test

Needs cmake, a C++14 compiler, and `uv`.

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
