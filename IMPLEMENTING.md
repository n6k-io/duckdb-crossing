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
	const void *Group();
};

struct ToySession {
	CrossingScan Run(ClientContext &context, const CrossingQuery &query);
	void Ddl(ClientContext &context, const CrossingDdl &ddl);
	vector<string> Tables(const string &schema);
	CrossingTable Describe(const string &schema, const string &name);
	void Commit();
	void Rollback();
};
```

No base class. `Crossing<ToyDB>` finds members by name. Required: `Session`,
`Tables`, `Describe`, `Plan`, `Begin`, `Session::Run`. Optional, with the
default when absent:

| member | absent |
|---|---|
| `Schemas` | one schema, `main` |
| `DescribeSchema` | no schema allows `CREATE` |
| `AcceptsCall` | no function, operator, aggregate or window crosses |
| `AcceptsType` | no expression crosses except a bare reference to a described column |
| `AcceptsOperator` | every operator accepted |
| `Detach` | nothing |
| `Group` | the source is its own group |
| `Session::Ddl` | no table may declare `ALTER` or `DROP`, no schema `CREATE`; one that does is refused at first use |
| `Session::Tables`, `Session::Describe` | the source's `Tables` and `Describe` |
| `Session::Commit`, `Session::Rollback` | nothing |

Members must be public. Neither type may be `final`. A member with the right name
and the wrong signature fails to compile with the member, the signature wanted
and the return type found. `test/contract/` holds wrong sources; `CMakeLists.txt`
holds the message each must produce.

A source may instead derive from `CrossingSource` and `CrossingSession` directly.

## Describe

Asked once per table, on first use, and kept until the attach or schema is refreshed.
A lookup under a different `owner` asks again.

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
- `key` — columns that identify a row. Without one, an update or delete that
  reads its rows, or a `MERGE` that updates or deletes, is a `BinderException`.
  See `CrossingTable::key_unique`.
- `verbs` — the only source of what a table allows; see `CrossingTable::verbs`.
  `ALTER` and `DROP` go through `Session::Ddl`.
- `returning` — the source can return the rows a write leaves.
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
the source. Indexes, sequences and schemas themselves are never created
through crossing.

`CREATE VIEW` is a `CREATE` whose `ddl.create` is a `CreateViewInfo` and whose
`ddl.plan` is the body, planned and folded as a read: floors only, the same plan
`Session::Run` would get. A body that does not fold wholly onto the source is
refused before the source is asked. A `VALUES` list in the body crosses as its
rows, unless a row holds something that changes between queries, such as
`now()`, which is refused. The plan's columns are
`CrossingViewColumnNames(view)`, in order; rendered Substrait names them `c0`,
`c1`, ... A source that stores views as SQL renders the plan rather than reusing
`view.query`, which names the target's catalog: `DecodeSubstraitView` gives the
body as a query with its columns named. A source that serves views lists and describes them as tables;
`DROP VIEW` and `CREATE OR REPLACE VIEW` need `DROP` on the described view.
A `CREATE` with no `ddl.plan` is a table.

`CREATE TABLE ... AS` is a `CREATE` followed by an `INSERT` of the gathered
rows into the new table, so it needs `CREATE` on the schema and `INSERT` on
the table the source then describes. The rows are always gathered on the
target.

## Plan

Asked once per read scan. An update or delete binds a scan of its table too, so it
is asked for those. A write needs no plan of its own: crossing builds it.

```cpp
CrossingPlan ToyDB::Plan(const CrossingPlanRequest &request) {
	return CrossingPlan::Of(MakeFloorNode(0, request.schema, request.table, names, types));
}
```

- A floor: the table named, emitting one column per described column, in order.
  Crossed work is stacked on it; it is still there in `Run`. Row filtering may be
  stacked on the floor here.
- `CrossingPlan::Declined(reason)` refuses; the reason is in the error.
- The plan names tables and never scans them; see `RequirePlaceholderScans` in
  `src/crossing_read.cpp`. Bind the table where the plan runs: `BindFloors` in
  `Run`. `FloorOf(node)` returns the schema and table named.
- Table indices you use are yours; crossing remaps them.

## Accepts

Each returns `CrossingVerdict::Yes()` or `No(reason)`. A no keeps the operator
carrying it on the target.

- `AcceptsCall` — asked per call, operator, aggregate and window expression.
  Column refs, constants, comparisons, `AND`/`OR`, casts, `BETWEEN`, `CASE`,
  `UNNEST` pass without asking. Lambdas, volatile and inconsistent expressions
  never cross.
- `AcceptsType` — asked about the type of every expression that would move,
  except a bare reference to a described column.
- `AcceptsOperator` — asked per operator after its expressions passed, except
  `CHUNK_GET` and `EMPTY_RESULT`, which are never asked about. A subquery
  is asked about as the operators DuckDB plans it to, such as `MARK` and `SINGLE`
  joins and materialized CTEs. A CTE ref is asked about only when its definition
  reads no source. A yes on a `SINGLE` join claims the source raises the multi-row
  error. While the target's `scalar_subquery_error_on_multiple_rows` is off, a
  `SINGLE` join is not asked about: it stays on the target in a read or write, and
  `CREATE VIEW` refuses a body holding one. A view is judged only when created, so
  turning the setting off later leaves its join on the source.

A yes claims the source computes the expression as DuckDB does. It means the
source can, not that it must: work over rows the target gathered stays on the
target.

## Group

Attaches whose sources return one non-null `Group()` cross together: a join or
view body over them folds into one fragment, carried by its first scan. See
`CrossingSource::Group` for what a group promises. A view's body must be in the
group of the attach that holds the view.

The fragment is read through one attach's session, so each floor must say where
its table lives: pass `catalog` to `MakeFloorNode` when the group spans
catalogs. Substrait then names the table `[catalog, schema, table]`, and
`BindFloors` binds it there instead of in the receiver's catalog. A receiver
decoding Substrait checks those catalogs with the `check` argument of
`DecodeSubstraitJson` / `DecodeSubstraitView`.

For which attach's session reads the fragment, see `ReadingCatalog` in
`src/crossing_read.cpp`.

## Run

Every read and write comes through one call. `WriteOf(query.plan)` tells them apart.

```cpp
CrossingScan ToySession::Run(ClientContext &, const CrossingQuery &query) {
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

`CrossingQuery` is in `include/crossing.hpp`. `query.plan.ToString()` prints the
plan; `SerializeCrossingPlan(query.plan)` gives bytes.

- Called once per scan operator. The chunk must carry exactly `query.types`.
  Pull outcomes: see `CrossingReadResult`.
- `partitions` > 1 lets DuckDB pull with that many threads. Together partitions
  produce every row once. Claiming, ordering and batch indexes: see `CrossingScan`.
- `waker.Wake()` may come from any thread. It is idempotent, safe before the pull
  returns and after the statement ended. Each pull gets its own waker.
- Scan and readers are destroyed when DuckDB stops asking.

### Running the plan on a DuckDB

```cpp
auto plan = DeserializeCrossingPlan(context, SerializeCrossingPlan(query.plan));
BindFloors(context, plan, "mycatalog");
auto result = SubmitCrossingPlan(context, std::move(plan));
result->ThrowIfError();
QueryResultStream<ChunkFormat> stream(std::move(result));
```

For `SubmitCrossingPlan` and `BindFloors`, see `include/crossing.hpp`. Calling
`Fetch` on the result itself collects every row first; move it into a
`QueryResultStream` to stream.

A `CrossingFloorResolver` may stand something else in for a floor, such as a
subquery with a row filter:

```cpp
BindFloors(context, plan, "mycatalog", [&](const CrossingFloor &floor) -> unique_ptr<TableRef> {
	return con.Table("mycatalog", floor.schema, floor.table)->Filter(policy)->GetTableRef();
});
```

## Write

```cpp
if (auto node = WriteOf(query.plan)) {
	CrossingScan scan;
	scan.open = [this, &query, node](ClientContext &, idx_t) -> CrossingReader {
		auto done = make_shared_ptr<bool>(false);
		return [this, &query, node, done](ClientContext &, DataChunk &chunk, CrossingWaker) {
			if (*done) {
				return CrossingReadResult::Done();
			}
			chunk.data[0].SetValue(0, Value::BIGINT(ToyWrite(conn, *node, query.plan)));
			chunk.SetChildCardinality(1);
			*done = true;
			return CrossingReadResult::Rows();
		};
	};
	return scan;
}
```

- `Run` is called once per write operator; not at all when the target gathered no
  rows. `query` outlives the scan.
- The readers produce one row of one BIGINT, the rows the source changed, which
  DuckDB reports. A write that has to wait returns `Wait()` as any reader does.
- `CrossingWriteNode` says what to write. Its child says with what. When every part
  of the feed can run on this source, the child is work reading this source's tables.
  Otherwise all of it runs on the target and the child holds the gathered rows, so
  `SeamRowsOf(*query.plan.children[0])` returns them. No child means the write takes
  nothing: a delete of every row, or `INSERT ... DEFAULT VALUES`.
- Render or bind the child, not the node: a renderer never sees a write node, and
  `BindFloors` refuses one.
- With `node->returning_columns`, the readers instead produce the rows the write
  left, those columns of them, and crossing holds them until the write has finished.
  Declare `CrossingTable::returning` to take these; without it `RETURNING` is refused
  before the source hears of it. `SubmitWrite` handles them for a source on DuckDB.
- A write that reads none of its rows: see `CrossingWriteTarget::reads_rows`.

### Running the write on a DuckDB

```cpp
auto target = CrossingWriteTargetOf("mycatalog", *node);
if (query.plan.children.empty()) {
	return SubmitWrite(context, target, unique_ptr<TableRef>());
}
auto &feed = *query.plan.children[0];
if (auto rows = SeamRowsOf(feed)) {
	return SubmitWrite(context, target, SeamRefOfRows(*rows));
}
auto received = DeserializeCrossingPlan(context, SerializeCrossingPlan(query.plan));
auto plan = std::move(received->children[0]);
BindFloors(context, plan, "mycatalog");
return SubmitWrite(context, target, std::move(plan));
```

For `SubmitWrite`, its seam and `CrossingWriteShaper`, see `include/crossing.hpp`.
Keys match with `IS NOT DISTINCT FROM`.

### Shipping the plan

```cpp
unique_ptr<LogicalOperator> plan = DeserializeCrossingPlan(context, bytes);
for (auto &use : CrossingTablesOf(*plan)) { ... }
```

The receiver is a DuckDB instance of the same commit with
`RegisterCrossingPlanFunctions(db)` called; `context` has a transaction open.

`RenderSubstraitJson(plan)` is complete: rows the plan holds, such as a `VALUES`
list joined to a table, travel in the JSON, and `DecodeSubstraitJson` needs
nothing else. Check `SeamRowsOf` on a write's child first: gathered rows can be any
size, and JSON is a poor way to move them.

Gathered rows may have spilled to disk, so walk `Chunks()` on them and send them in whatever batches the transport needs. Crossing sets no size
limit. A source that cannot move them all fails the statement with its reason.

## Transactions

`Begin` is called on a DuckDB transaction's first `Run` or `Ddl`. Every
`Run` in that transaction goes through the returned session; when
the transaction resolves, `Commit` or `Rollback` is called and the session
destroyed. A `Commit` that throws fails the statement; nothing is undone
elsewhere. A `Rollback` that throws is discarded.

A write must not be durable until `Commit`. This holds in autocommit too, where
the statement's session resolves after its last reader returned. A statement can
fail after a write's reader returned: a later `MERGE` action can fail, and crossing can
find that a key without `key_unique` matched more rows than it sent. `Rollback`
must then leave the source as it was. A source that cannot hold a write until
`Commit` must not declare a key without `key_unique`, and a `MERGE` of more than
one action on it is not atomic.

## Detach

`Detach(context)` runs on `DETACH` from that connection. Open sessions are
rolled back and destroyed afterwards. The source object is destroyed later.

## Threads

Any member may be called from any thread. Unless stated, calls may overlap with
each other and themselves.

| call | when |
|---|---|
| `Schemas`, `Tables`, `DescribeSchema` | cached per attach or schema, asked again after `Refresh`; serialized per attach; must not re-enter crossing |
| `Describe` | cached per table, asked again after `Refresh`; serialized per schema; must not re-enter crossing |
| `Plan`, `Accepts*` | binding and optimization, concurrent across statements |
| `Begin` | once per transaction, on the thread that first needs the session (the binding thread for `Ddl`); different transactions may overlap; must not re-enter crossing |
| `Detach` | once, DETACH thread |
| `Session::Run` | once per scan or write operator, and per session more than once (`MERGE`); two scans of one statement may overlap on one session |
| `CrossingScan::open` | once per partition, by the claiming thread; opens overlap |
| reader | see `CrossingReader`; partitions run concurrently |
| `Session::Ddl` | once per DDL statement, on the binding thread; may repeat per session |
| `Session::Tables`, `Session::Describe` | after `Ddl` in that session, once per schema or table until the next `Ddl` |
| `Commit`, `Rollback` | once, after every reader returned |
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
once per `Crossing<S>` per instance, the optimizer pass. The catalog holds the source's tables
only; `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE VIEW` and `DROP VIEW`
go to the source when the verb is declared, and everything else is refused.

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

- `LookupTable(schema, owner, name, transaction)`, `ScanTables`, `Described` — entries and
  descriptions, cached; `Refresh()` / `Refresh(schema)` forget them.
- `Ddl(context, transaction, owner, ddl)` — checks the verb and runs it in the
  transaction's session. That transaction reads the schema through its session;
  the schema is forgotten once it resolves. `DescribedSchema(schema)` is what
  the schema declares. `PlanCreateTableAs(...)` is what a catalog's own
  `PlanCreateTableAs` forwards to; `CreateView(context, transaction, owner,
  info)` what its schema's `CreateView` forwards to.
- `ServesTable`, `ThrowIfServed`, `ThrowIfSchemaServed` — for a catalog that
  refuses `DROP`/`ALTER` instead.
- `Session(context, transaction)`; `Release(transaction)` then
  `Commit`/`Rollback` on the result, before a base manager frees the
  `Transaction`; or `Commit(transaction)`/`Rollback(transaction)`.
- `OnDetach` calls `attach.Detach(context)`.
- `attach.Source<ToyDB>()` returns the native source.
- Call `Crossing<ToyDB>::RegisterPass(db)`, or every read and write of a served
  table fails. Repeat calls do nothing.

`test/own_catalog/` is a complete example.

### Building

```cmake
crossing_add_to_target(toy_extension)
```

Adds sources and include path. `crossing_sources` and `crossing_includes` give
the lists. A source includes `crossing.hpp`. Its own catalog also needs
`crossing_attach.hpp`. Substrait needs `crossing_substrait.hpp`, whose sources and
include path come from `crossing_substrait_sources` and `crossing_substrait_includes`.
Streaming a result needs DuckDB's `duckdb/main/query_result_stream.hpp`.

## Migrating from `Read` and `Write`

- `Session::Read` and `Session::Write` are one `Session::Run`. Branch on
  `WriteOf(query.plan)`.
- A write's `CrossingWriter` is a one-partition `CrossingScan` whose reader produces
  the count as one BIGINT.
- `query.kind`, `written`, `key_columns`, `set_columns` and `reads_rows` move to
  `CrossingWriteNode`. `CrossingWriteTargetOf` builds the target, `reads_rows` included.
- The plan a write carries is the node. Use its child where you used `query.plan`,
  and an absent child where you checked `query.types.empty()`.
- `ExecuteWrite` is `SubmitWrite`, which returns the result rather than a count.
- `RETURNING` now runs on the source: set `CrossingTable::returning` and produce the
  rows `node->returning_columns` names, or the statement is refused.
- `Plan` answers for scans only, so drop its write branch and `MakeSeamNode`. A
  source refuses a write through `CrossingTable::verbs`.
