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

## What runs where

Query shape decides, and table sizes never do.

- Work on one source's tables runs on that source.
- The first inner join between a source's table and rows that read nothing from any
  source runs on the source, when the source accepts it and a condition links the two
  sides. Those rows are read first and sent over. `far JOIN a JOIN b` sends `a`, and
  `far JOIN (a JOIN b)` sends the join's result.
- Later joins, outer, semi and anti joins, joins with no condition linking the sides,
  and joins using a CTE or inside a correlated subquery run near.

Refused with an error:

- `RETURNING` on a table whose source can't return rows.
- `DEFAULT` as a value on a source's table.
- `ON CONFLICT` that leaves out a column.
- `MERGE ... INSERT DEFAULT VALUES`.
- A write to a source's table inside a `WITH` clause.
- A view over rows that aren't on its source.

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
