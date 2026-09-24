# Implementing a source

Crossing moves part of a DuckDB query onto another database. You write one plain
type; it becomes attachable:

```sql
ATTACH 'toy.example.com' AS toy (TYPE toydb);
SELECT upper(name) FROM toy.orders WHERE amt > 100 LIMIT 10;
```

## Interface

```cpp
struct ToyDB {
	using Session = ToySession;

	vector<string> Schemas();
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	CrossingSchema DescribeSchema(const string &schema);
	CrossingPlan Plan(const CrossingPlanRequest &request);

	CrossingVerdict AcceptsCall(const Expression &expr);
	CrossingVerdict AcceptsType(const LogicalType &type);
	CrossingVerdict AcceptsOperator(const LogicalOperator &op);

	unique_ptr<Session> Begin(ClientContext &context);
	void Detach(ClientContext &context);
};

struct ToySession {
	CrossingScan Read(ClientContext &context, const CrossingQuery &query);
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	void Commit();
	void Rollback();
};
```

No base class. `Crossing<ToyDB>` finds members by name. Required: `Session`,
`Tables`, `Describe`, `Plan`, `Begin`, `Session::Read`. Optional, with the
default when absent:

| member | absent |
|---|---|
| `Schemas` | one schema, `main` |
| `DescribeSchema` | no schema allows `CREATE` |
| `AcceptsCall` | no function, operator, aggregate or window crosses |
| `AcceptsType` | no expression crosses |
| `AcceptsOperator` | every operator accepted |
| `Detach` | nothing |
| `Session::Write` | no table may declare a write verb; one that does is refused at first use |
| `Session::Ddl` | no table may declare `ALTER` or `DROP`, no schema `CREATE`; one that does is refused at first use |
| `Session::Tables`, `Session::Describe` | the source's `Tables` and `Describe` |
| `Session::Commit`, `Session::Rollback` | nothing |

Members must be public. Neither type may be `final`. A member with the right name
and the wrong signature fails to compile with the member, the signature wanted
and the return type found. `test/contract/` holds one wrong source per message.

A source may instead derive from `CrossingSource` and `CrossingSession` directly.

## Describe

Asked once per table, on first use, cached for the life of the attach.

```cpp
CrossingTable ToyDB::Describe(const string &schema, const string &name) {
	CrossingTable table;
	table.name = name;
	table.Column("id", LogicalType::INTEGER);
	table.Column("amt", LogicalType::INTEGER);
	table.key = {"id"};
	table.verbs = {CrossingVerb::SELECT, CrossingVerb::INSERT, CrossingVerb::UPDATE};
	return table;
}
```

- `column_names` / `column_types` — in order. Every column position in a query
  means this order.
- `key` — columns that identify a row. Required for a table allowing `UPDATE`
  or `DELETE_`. `key_unique = true` vouches uniqueness; only then may a keyed
  write run wholly on the source.
- `verbs` — a verb absent here is refused before the statement runs. The only
  source of what a table allows. `DELETE_` has a trailing underscore. `ALTER`
  and `DROP` go through `Session::Ddl`.
- `constraints` — constraints the source enforces.

`DescribeSchema(schema)` returns a `CrossingSchema` the same way; `CREATE`
lives there, since the table does not exist yet. Asked once per schema and
kept until the schema is refreshed.

## Ddl

```cpp
void ToySession::Ddl(ClientContext &, const CrossingDdl &ddl) {
	switch (ddl.verb) {
	case CrossingVerb::CREATE:  ToyCreate(conn, *ddl.create); break;
	case CrossingVerb::ALTER:   ToyAlter(conn, *ddl.alter); break;
	default:                    ToyDrop(conn, *ddl.drop); break;
	}
}
```

`CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` on a served schema or table
arrive as DuckDB's parsed `CreateInfo`, `AlterInfo` or `DropInfo`, in the
session of the statement's transaction. The verb is checked against
`DescribeSchema` or `Describe` first; the source is never asked about a verb it
did not declare. After the call the schema is listed and described again
through `Session::Tables` and `Session::Describe` for the rest of that
transaction, so the transaction sees its own uncommitted DDL and no other
transaction's; once it commits or rolls back the schema is listed again through
the source. Views, indexes, sequences and schemas themselves are
never created through crossing.

`CREATE TABLE ... AS` is a `CREATE` followed by an `INSERT` of the gathered
rows into the new table, so it needs `CREATE` on the schema and `INSERT` on
the table the source then describes. The rows are always gathered on the
target.

## Plan

Asked once per read scan and once per write operator (`MERGE` plans once per
action).

```cpp
CrossingPlan ToyDB::Plan(const CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		return CrossingPlan::Of(MakeFloorNode(0, request.schema, request.table, names, types));
	}
	return CrossingPlan::Of(MakeSeamNode(0, request.seam.types));
}
```

- `SELECT` — a floor: the table named, emitting one column per described
  column, in order. Crossed work is stacked on it; it is still there in `Read`.
  Row filtering may be stacked on the floor here.
- Write — a seam node: where the rows go. `request.seam.key_columns` then
  `set_columns` are the seam's column order: insert = row image in `Describe`
  order; update = key then set columns; delete = key.
- `CrossingPlan::Declined(reason)` refuses; the reason is in the error.
- The plan names tables; it never scans them. A `LogicalGet` of your own storage
  is bound to the connection that made it and cannot travel, so crossing refuses
  a plan holding one. Bind the table where the plan runs: `BindFloors` in
  `Read`, `ExecuteWrite` in `Write`. `FloorOf(node)` returns the schema and
  table named.
- Table indices you use are yours; crossing remaps them.

## Accepts

Each returns `CrossingVerdict::Yes()` or `No(reason)`. A no keeps the operator
carrying it on the target.

- `AcceptsCall` — asked per call, operator, aggregate and window expression.
  Column refs, constants, comparisons, `AND`/`OR`, casts, `BETWEEN`, `CASE`,
  `UNNEST` pass without asking. Subqueries, lambdas, volatile and inconsistent
  expressions never cross.
- `AcceptsType` — asked about the type of every expression that would move,
  except a bare reference to a described column.
- `AcceptsOperator` — asked per operator after its expressions passed.

A yes claims the source computes the expression as DuckDB does.

## Read

```cpp
CrossingScan ToySession::Read(ClientContext &, const CrossingQuery &query) {
	auto cursor = make_shared_ptr<ToyCursor>(ToyOpen(conn, query));
	CrossingScan scan;
	scan.open = [cursor](ClientContext &, idx_t) -> CrossingReader {
		return [cursor](ClientContext &, DataChunk &chunk, CrossingWaker) {
			return ToyFetch(*cursor, chunk) ? CrossingReadResult::Rows() : CrossingReadResult::Done();
		};
	};
	return scan;
}
```

`CrossingQuery`: `kind`, `plan` (unoptimized tree, run as is), `types`,
`ordered`, `tables`, `written`, `key_columns`, `set_columns`. `plan.ToString()`
prints it; `SerializeCrossingPlan(plan)` gives bytes.

- Called once per scan operator. The chunk must carry exactly `query.types`.
  `Rows()` with an empty chunk ends the partition.
- `partitions` > 1 lets DuckDB pull with that many threads; each thread opens
  and drains one partition at a time. Together partitions produce every row
  once. A query with `ordered` set must have one partition.
- A reader with nothing yet returns `Wait()` and calls `waker.Wake()` later from
  any thread. Wake is idempotent, safe before the pull returns and after the
  statement ended. Each pull gets its own waker.
- Scan and readers are destroyed when DuckDB stops asking.

### Running the plan on a DuckDB

```cpp
auto plan = DeserializeCrossingPlan(context, SerializeCrossingPlan(query.plan));
BindFloors(context, plan, "mycatalog");
context.PendingQuery(make_uniq<LogicalPlanStatement>(std::move(plan)), QueryParameters(true));
```

`BindFloors` replaces each floor with the table it names, bound on `context`,
which must have a transaction open. The floor keeps its table index and column
bindings, so the plan above it is untouched. A table whose columns no longer
match the floor is a `CatalogException`. A resolver may stand something else
in for a floor, such as a subquery with a row filter:

```cpp
BindFloors(context, plan, "mycatalog", [&](const CrossingFloor &floor) -> unique_ptr<TableRef> {
	return con.Table("mycatalog", floor.schema, floor.table)->Filter(policy)->GetTableRef();
});
```

## Write

```cpp
CrossingWriter ToySession::Write(ClientContext &, const CrossingQuery &query) {
	return [this, &query](ClientContext &, CrossingWaker) {
		return CrossingWriteResult::Done(ToyRun(conn, query));
	};
}
```

- Called once per write operator; not at all when the target gathered no rows.
  `query` outlives the writer.
- The plan is the one from `Plan` with the seam filled: by a read of this
  source's tables when the rows came from it, else by gathered rows.
  `SeamRowsOf(op)` returns those rows.
- `Wait()` / `waker.Wake()` as for readers.
- `Done(n)` is the rows the source changed, which DuckDB reports.
- `RETURNING` never runs wholly on the source.

### Running the write on a DuckDB

```cpp
CrossingWriteTarget target {"mycatalog", query.written.schema, query.written.table, query.kind,
                            query.key_columns, query.set_columns};
if (auto rows = SeamRowsOf(query.plan)) {
	return ExecuteWrite(context, target, SeamRefOfRows(*rows));
}
auto plan = DeserializeCrossingPlan(context, SerializeCrossingPlan(query.plan));
BindFloors(context, plan, "mycatalog");
return ExecuteWrite(context, target, std::move(plan));
```

`ExecuteWrite` builds the INSERT, UPDATE or DELETE as parser nodes over the
seam, binds it on `context` (a transaction must be open) and runs it, returning
the rows changed. Keys match with `IS NOT DISTINCT FROM`. The seam is any
`TableRef` whose columns are, by position, the key columns then the set
columns, or a bound plan producing them. A shaper sees the statement before it
binds, as `CrossingWriteStatement` with the seam's alias and column names, to
add a row policy or a check.

### Shipping the plan

```cpp
unique_ptr<LogicalOperator> plan = DeserializeCrossingPlan(context, bytes);
for (auto &use : CrossingTablesOf(*plan)) { ... }
```

The receiver is a DuckDB instance of the same commit with
`RegisterCrossingPlanFunctions(db)` called; `context` has a transaction open.

## Transactions

`Begin` is called the first time a DuckDB transaction touches the source. Every
`Read` and `Write` in that transaction goes through the returned session; when
the transaction resolves, `Commit` or `Rollback` is called and the session
destroyed. A `Commit` that throws fails the statement; nothing is undone
elsewhere. A `Rollback` that throws is discarded.

## Detach

`Detach(context)` runs on `DETACH` from that connection. Open sessions are
rolled back and destroyed afterwards. The source object is destroyed later.

## Threads

Any member may be called from any thread. Unless stated, calls may overlap with
each other and themselves.

| call | when |
|---|---|
| `Schemas`, `Tables`, `Describe` | once per attach (again after `Refresh`), cached; not serialized |
| `Plan`, `Accepts*` | binding and optimization, concurrent across statements |
| `Begin` | once per transaction, on a worker thread; different transactions may overlap; must not re-enter crossing |
| `Detach` | once, DETACH thread |
| `Session::Read` | once per scan operator; two scans of one statement may overlap on one session |
| `CrossingScan::open` | once per partition, by the claiming thread; opens overlap |
| reader | one thread at a time; thread may change after `WAIT`; partitions run concurrently |
| `Session::Write` | once per write operator; may repeat per session (`MERGE`) |
| writer | one thread at a time until `DONE`; thread may change after `WAIT` |
| `Session::Ddl` | once per DDL statement, on the binding thread; may repeat per session |
| `Session::Tables`, `Session::Describe` | after `Ddl` in that session, once per schema or table until the next `Ddl` |
| `Commit`, `Rollback` | once, after every reader and writer returned |
| `Waker::Wake` | any thread, any time |

## Registering

```cpp
void ToyExtension::Load(ExtensionLoader &loader) {
	Crossing<ToyDB>::Register(loader, "toydb", [](ClientContext &, AttachInfo &info) {
		return make_uniq<ToyDB>(info.path);
	});
}
```

`"toydb"` is `ATTACH ... (TYPE toydb)`. `info.path` and `info.options` are
passed through; the path is never opened as a file. The factory runs once per
`ATTACH`. Registering installs the storage extension, transaction manager and,
once per instance, the optimizer pass. The catalog holds the source's tables
only; `CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` go to the source when the
verb is declared, and everything else is refused.

### Identity

`Crossing<S>` is the identity: one address per `Crossing<S>` per loaded copy of
crossing, compared by address. Every node crossing plants carries it; a pass
touches only nodes of its own identity. Two extensions each compiling crossing
in coexist; both passes run on every plan. Build against the same DuckDB commit
as the extension.

### Your own catalog

`crossing_attach.hpp` gives `CrossingAttach`, one per `ATTACH`: holds the
source, lists and describes tables, hands out entries, keeps each transaction's
session.

```cpp
class MyCatalog : public DuckCatalog, public CrossingAttachOwner {
	CrossingAttach attach;
	MyCatalog(AttachedDatabase &db, unique_ptr<ToyDB> source)
	    : DuckCatalog(db), attach(db, Crossing<ToyDB>::Adapt(std::move(source))) {}
	CrossingAttach &Attach() override { return attach; }
};
```

- `LookupTable(schema, owner, name)`, `ScanTables`, `Described` — entries and
  descriptions, cached; `Refresh()` / `Refresh(schema)` forget them.
- `Ddl(context, transaction, owner, ddl)` — checks the verb, runs it in the
  transaction's session, forgets the schema. `DescribedSchema(schema)` is what
  the schema declares. `PlanCreateTableAs(...)` is what a catalog's own
  `PlanCreateTableAs` forwards to.
- `ServesTable`, `ThrowIfServed`, `ThrowIfSchemaServed` — for a catalog that
  refuses `DROP`/`ALTER` instead.
- `Session(context, transaction)`; `Release(transaction)` then
  `Commit`/`Rollback` on the result, before a base manager frees the
  `Transaction`; or `Commit(transaction)`/`Rollback(transaction)`.
- `OnDetach` calls `attach.Detach(context)`.
- `attach.Source<ToyDB>()` returns the native source.
- Call `Crossing<ToyDB>::RegisterPass(db)` once, or nothing crosses.

`test/own_catalog/` is a complete example.

### Building

```cmake
crossing_add_to_target(toy_extension)
```

Adds sources and include path. `crossing_sources` and `crossing_includes` give
the lists. `crossing.hpp` is the only header to include.
