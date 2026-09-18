# Implementing a source

Crossing is a library that moves part of a DuckDB query onto another database.
You implement one class against it, and your database becomes attachable:

```sql
ATTACH 'toy.example.com' AS toy (TYPE toydb);
SELECT upper(name) FROM toy.orders WHERE amt > 100 LIMIT 10;
```

DuckDB would ordinarily read every row of `orders` and do the filtering, the
`upper()` and the limit itself. Crossing hands you that work as a query instead,
and you return ten rows.

Three things to write.

1. **Describe your tables** — columns, and who may do what to which rows.
2. **Say what your database can compute.**
3. **Run the query you are handed.**

## The whole interface

```cpp
class ToyDB : public CrossingSource {
	// 1. describe
	vector<string> Schemas() override;
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	CrossingPlan Plan(const CrossingPlanRequest &request) override;

	// 2. compute
	CrossingVerdict AcceptsCall(const Expression &expr) override;
	CrossingVerdict AcceptsType(const LogicalType &type) override;
	CrossingVerdict AcceptsOperator(const LogicalOperator &op) override;

	// 3. run
	unique_ptr<CrossingSession> Begin(ClientContext &context) override;
};

class ToySession : public CrossingSession {
	CrossingScan Read(ClientContext &context, const CrossingQuery &query) override;
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query) override;
	void Commit() override;
	void Rollback() override;
};
```

`Tables`, `Describe`, `Plan` and `Begin` must be implemented on the source, and
`Read` on the session. `Schemas` has a base implementation returning `main`.
`AcceptsCall` and `AcceptsType` say no by default, so a source that computes
nothing leaves them alone; `AcceptsOperator` says yes by default. `Write` on the session refuses by default, so a
read-only source leaves it alone; `Commit` and `Rollback` do nothing by
default, for a source with no transaction of its own.

In the examples below, anything named `Toy...` is a stand-in for your own code
talking to your own database. Crossing never sees it.

---

## 1. Describe your tables

`Schemas` and `Tables` are what DuckDB lists when someone asks what is in the
attached database. `Describe` is asked once per table, the first time someone
names one, and is where you say what the table is and who may do what to it.

What you answer is kept for the life of the attach. There is no invalidation: a
table that appears on your side afterwards is not there until the next attach,
and a policy you tighten afterwards does not reach a statement already bound.

```cpp
vector<string> ToyDB::Schemas() {
	return ToyListSchemas();
}

vector<string> ToyDB::Tables(const string &schema) {
	return ToyListTables(schema);
}

CrossingTable ToyDB::Describe(const string &schema, const string &name) {
	CrossingTable table;
	table.name = name;

	table.Column("id", LogicalType::INTEGER);
	table.Column("name", LogicalType::VARCHAR);
	table.Column("amt", LogicalType::INTEGER);
	table.Column("tenant", LogicalType::INTEGER);

	table.key = {"id"};

	table.verbs = {CrossingVerb::SELECT, CrossingVerb::INSERT, CrossingVerb::UPDATE};

	return table;
}
```

A description is three things.

**`column_names` / `column_types`** — the table's columns, in order, with their
DuckDB types. `Column(name, type)` appends to both.

**`key`** — the columns that identify a row. An update or a delete addresses rows
by it, so a table that allows either must declare one. `key_unique = true`
vouches for uniqueness: only then may an update or a delete run wholly on
the source, since nothing then counts the rows the target would have sent.

**`verbs`** — a verb not listed cannot happen. Not "it errors": there is no
query for it, and the attempt is refused when the statement is bound. Above,
`DELETE` is absent, so nothing can delete from this table. The enumerator is
spelled `CrossingVerb::DELETE_`, with the underscore: `DELETE` is a macro in some Windows
SDK headers.

**`constraints`** — constraints the source enforces. Declaring one your source
does not enforce claims a guarantee nothing keeps, so only add what is real.

### Row policies

Which rows a verb may reach, and which rows a write may leave behind, are yours
to enforce, and the place is `Plan`. A predicate over the table's columns only
means anything against the scan those columns come from, and that scan is the one
you build. Think of Postgres's `CREATE POLICY`: its `USING` clause goes at the
bottom of your read plan and in the `WHERE` of your update and delete; its
`WITH CHECK` clause guards what your insert and update write. `USING` alone
stops an update reaching another tenant's row but leaves it free to move one of
your rows *to* another tenant, so a writable table wants both.

Crossing never sees a policy. It sees a plan with a filter at its floor, which
nothing above can lift.

### The plans everything is built on

`Plan` is asked for one plan per verb on one table, each time a statement binds
that verb on it — a read is planned once per scan, a write once per statement.

```cpp
CrossingPlan ToyDB::Plan(const CrossingPlanRequest &request) {
	switch (request.verb) {
	case CrossingVerb::SELECT:
		return CrossingPlan::Of(ToyScanOf(request.schema, request.table));   // with your USING policy on it
	case CrossingVerb::INSERT:
	case CrossingVerb::UPDATE:
	case CrossingVerb::DELETE_:
		return CrossingPlan::Of(ToyWriteOf(request));                       // with a seam where the rows go
	}
}
```

For `SELECT` it is your own read of the table. Everything that crosses is
stacked on top of it, and it is still there when the query comes back to you.
This is where the `USING` policy goes for a read: whatever you return is the
floor, so a filter you put here is one nothing above can lift. The node has to
emit one column per column you described, in that order.

For a write it is the statement with a hole where the rows go: a *seam*, made by
`MakeSeamNode(table_index, request.seam.types)`. `request.seam.key_columns` and
`request.seam.set_columns` say what the seam's columns are, in order: an insert's
seam is the row image in `Describe` order; an update's is the key columns then
the columns it sets; a delete's is the key columns. Put your `USING` policy in
the statement's `WHERE` and your `WITH CHECK` around what it writes.

For any verb, return `CrossingPlan::Declined(reason)` to refuse. The reason is
in the error the statement fails with, and in `EXPLAIN` for a write.

Beyond that, what a plan must be depends on what you do with the query later. A
source that runs the plan it is handed needs nodes that can actually run. A
source that renders the tree to its own query language needs only something
that names the table, because nobody executes it, and crossing has those:

```cpp
CrossingPlan ToyDB::Plan(const CrossingPlanRequest &request) {
	if (request.verb == CrossingVerb::SELECT) {
		return CrossingPlan::Of(MakeFloorNode(0, request.schema, request.table, names, types));
	}
	return CrossingPlan::Of(MakeSeamNode(0, request.seam.types));
}
```

`MakeFloorNode` is a scan of your table that emits the columns you describe, in
that order. A seam node on its own is a whole write plan: the rows arrive in it
and the query's `kind`, `tables`, `key_columns` and `set_columns` say what to do
with them. When you walk a plan later, `FloorOf(node)` gives you back the schema
and table you named, or null for anything else. Whatever table indices you use
are yours: crossing moves them out of the way before the plan meets DuckDB's.

Whatever you return may be handed back inside a bigger tree, so nothing above
it should rely on its index. What you get in `Read` is your floor with the
crossed work stacked on it; in `Write` it is your seam filled — by a
`LogicalColumnDataGet` of rows the target gathered, or by a read of your own
tables when the rows came from this source.

---

## 2. What ToyDB can compute

Crossing asks two questions: whether ToyDB can compute an expression, and whether
it can represent a type. Each answer is a `CrossingVerdict`: yes, or no with a
reason. Say no to either and whatever needed it stays in DuckDB; the reason is
what EXPLAIN reports when a write could not run on the source.

```cpp
CrossingVerdict ToyDB::AcceptsCall(const Expression &expr) {
	string name;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		name = expr.Cast<BoundFunctionExpression>().function.name;
		break;
	case ExpressionClass::BOUND_AGGREGATE:
		name = expr.Cast<BoundAggregateExpression>().function.name;
		break;
	default:
		return CrossingVerdict::Yes();
	}
	return ToyHasFunction(name) ? CrossingVerdict::Yes() : CrossingVerdict::No("ToyDB has no " + name);
}

CrossingVerdict ToyDB::AcceptsType(const LogicalType &type) {
	if (type.IsNumeric() || type.id() == LogicalTypeId::VARCHAR) {
		return CrossingVerdict::Yes();
	}
	return CrossingVerdict::No("ToyDB cannot hold a " + type.ToString());
}
```

`AcceptsCall` is asked about one expression at a time, and only about
kinds crossing is willing to move at all — column references, constants,
comparisons, `AND`/`OR`, operators (`IS NULL`, `IN`, `NOT`), casts, `BETWEEN`,
`CASE`, `UNNEST`, calls, and window functions. Anything else, a subquery or a
lambda say, is refused before you see it, which is why `default: Yes()` above is
safe. In practice you answer for the calls and let the rest through. A window
function arrives as a `BoundWindowExpression`: `aggregate` names the function
for `sum(...) OVER`, and the expression type does for `row_number` and its kin.

Two kinds are refused whatever you answer: an expression that is volatile, and
one that is not consistent. Evaluated once here and once on ToyDB they are two
answers to one question, and which one the query got would depend on what
crossed.

`AcceptsType` is asked about the type of every expression before it
moves. Say no and the operator carrying that type stays in DuckDB. Without it, a `STRUCT`, an
`ENUM` or an extension type reaches ToyDB inside a cast or a constant that no
other question would have caught. It is not asked about the column types you
declared in `Describe` — those you chose, so they are taken as given.

`AcceptsOperator` is asked about each operator crossing would move, after its
expressions passed. It says yes by default; a source that renders to a language
without, say, grouping sets or a join type answers no there:

```cpp
CrossingVerdict ToyDB::AcceptsOperator(const LogicalOperator &op) {
	string reason;
	return SubstraitCanRenderOperator(op, reason) ? CrossingVerdict::Yes() : CrossingVerdict::No(reason);
}
```

Saying yes to `upper` claims ToyDB's `upper` is DuckDB's `upper`. If it folds
case differently, answers come back wrong rather than slow. Say no when unsure.

---

## 3. Run the query

A query is DuckDB's operator tree, unoptimized. Walk it and emit ToyDB's own
query language, or serialize it and send it to something that speaks DuckDB
operators.

```cpp
struct CrossingQuery {
	//! What this query does. SELECT on a read, the write verb on a write.
	CrossingVerb kind;

	//! The tree. On a read, your floor with the crossed work on it. On a write,
	//! the statement with its seam already filled. Run it as it is.
	const LogicalOperator &plan;

	//! The row types: what a read produces, what a write is handed.
	vector<LogicalType> types;

	//! The plan sorts its rows and the target keeps that order. Answer with one
	//! partition; a scan offering more is refused.
	bool ordered;

	//! Which of your tables it touches. Each entry is a table name and the
	//! columns of it this query reads.
	vector<CrossingTableUse> tables;

	//! The columns an update or a delete addresses rows by. An insert names none.
	vector<string> key_columns;

	//! The columns a write sets. An insert names every column, in the order Describe
	//! declared them. An update names the columns it sets. A delete names none.
	vector<string> set_columns;
};
```

`plan.ToString()` prints the tree, for logs and tests. `SerializeCrossingPlan(plan)`
is the tree as bytes, for sending to a process that speaks DuckDB operators.

Reads and writes go through a session, the object `Begin` returns (see
Transactions below). A read returns a scan, and the scan opens one reader per
partition:

```cpp
//! Fill `chunk` with the next rows.
using CrossingReader = std::function<CrossingPull(ClientContext &context, DataChunk &chunk, CrossingWaker waker)>;

struct CrossingScan {
	//! How many independent streams the rows come in. One by default.
	idx_t partitions = 1;
	//! A reader for one of them. Called once per partition.
	std::function<CrossingReader(ClientContext &context, idx_t partition)> open;
};
```

```cpp
CrossingScan ToySession::Read(ClientContext &context, const CrossingQuery &query) {
	auto cursor = make_shared_ptr<ToyCursor>(ToyOpen(conn, query));
	CrossingScan scan;
	scan.open = [cursor](ClientContext &, idx_t) -> CrossingReader {
		return [cursor](ClientContext &, DataChunk &chunk, CrossingWaker) {
			return ToyFetch(*cursor, chunk) ? CrossingPull::Rows() : CrossingPull::Done();
		};
	};
	return scan;
}
```

A reader holds whatever it needs in its closure. `std::function` copies what it
holds, so anything that cannot be copied goes behind a `shared_ptr`.

DuckDB asks for one chunk at a time, so a large result is never materialised in
full. When DuckDB has stopped asking — a `LIMIT` satisfied above you, or a
cancelled statement — the scan and its readers are destroyed, and releasing
whatever they hold is the whole of what you owe them.

`Read` is called once per scan DuckDB opens — so more than once for one query,
and from whichever thread opens the scan. The scan belongs to one DuckDB scan
and is not shared. The chunk a reader fills must carry exactly `query.types`;
anything else fails the scan.

A scan that can split its rows into independent streams says how many with
`partitions`. DuckDB then pulls with up to that many threads at once; each
thread takes a partition, calls `open` for it, and pulls that reader to
exhaustion before taking another. A reader is only ever used by the thread that
opened it, so it needs no locking of its own; `open` can be called from
several threads at once. Together the partitions must produce every row exactly
once; the order across them is not kept, so a query with `ordered` set must be
answered with one partition, and crossing refuses a scan that offers more.

A reader that has nothing yet — a frame still on the wire — keeps the `waker` it
was handed, answers `CrossingPull::Wait()`, and calls `waker.Wake()` from
whatever thread the frame arrives on. Nothing of DuckDB's is held in between: the
scan is parked, its worker goes on to other work, and the reader is asked again
once woken. A reader may answer `Wait` as many times as it needs before `Rows`.
Each pull comes with its own waker, for that pull's `Wait` only. Copies of a waker
share the wake. The first `Wake()` counts and later ones do nothing, as does a
`Wake()` after the statement stopped asking, so a source may fire it late without
checking. A `Wake()` called before the pull returns is not lost; the reader is
simply asked again at once.

```cpp
return [cursor](ClientContext &, DataChunk &chunk, CrossingWaker waker) {
	if (!ToyReady(*cursor)) {
		ToyOnReady(*cursor, [waker]() { waker.Wake(); });
		return CrossingPull::Wait();
	}
	return ToyFetch(*cursor, chunk) ? CrossingPull::Rows() : CrossingPull::Done();
};
```

The source object is shared by every connection on the attach. Crossing
serialises `Describe` and `Begin` and nothing else: `Plan`, `AcceptsCall` and
`AcceptsType` can run concurrently, so a source that cannot answer two at once
must lock itself. A session belongs to one DuckDB transaction; `Read` and
`Write` on it may still be called from different threads of that transaction's
connection.

A write returns a writer, which runs the plan it is handed when asked:

```cpp
CrossingWriter ToySession::Write(ClientContext &context, const CrossingQuery &query) {
	return [this, &query](ClientContext &, CrossingWaker) {
		return CrossingWriteResult::Done(ToyRun(conn, query));
	};
}
```

A writer is asked until it answers `Done`. One that has sent the statement and is
still waiting on the source answers `CrossingWriteResult::Wait()` and calls
`waker.Wake()` when the answer is in, exactly as a reader does; the statement is
parked meanwhile. The writer is destroyed when the statement stops asking —
finished, failed, or cancelled — and releasing whatever it holds is all it owes.
`query` outlives the writer, so it may be captured by reference.

The plan is the one you returned from `Plan`, with the seam filled. When the rows
were something ToyDB could produce itself — `INSERT INTO toy.a SELECT * FROM
toy.b`, or `UPDATE toy.a SET amt = 0 WHERE tenant = 1` on a table with a
`key_unique` key — the seam holds that read, and nothing crosses at all. Otherwise the
seam holds the rows the target gathered, already in the seam's column order and
types; `SeamRowsOf(op)` hands them to you as a `ColumnDataCollection` when `op`
is that node. `types` is that shape.

`context` on `Read`, the reader and `Write` is the target connection running the
statement — for cancellation checks and for whatever the host platform demands
of the calling thread.

`Done` carries the number of rows the statement changed, which is what DuckDB
reports back to the user. It is what ToyDB actually changed, not the number of
rows it was handed: for an update or a delete those are different numbers, and
the difference is what catches a key that identifies more than one row.

Unlike `Read`, `Write` is called once per statement.

### Shipping the plan

A source that runs the plan somewhere else — another process, another machine —
sends `SerializeCrossingPlan(query.plan)` and rebuilds it there:

```cpp
unique_ptr<LogicalOperator> plan = DeserializeCrossingPlan(context, bytes);
for (auto &use : CrossingTablesOf(*plan)) { ... }   // the floors, and the columns read from each
```

The receiving side needs a DuckDB instance of the same commit with
`RegisterCrossingPass(db)` called on it, so the floor and seam nodes resolve by
name; `context` is a connection on that instance with a transaction open.
`FloorOf` and `SeamRowsOf` work on the rebuilt tree exactly as on the original,
and a seam the target filled arrives with its rows.

### Transactions

`Begin` is what makes a target transaction's writes one change on the source.
Crossing calls it the first time a DuckDB transaction touches you at all — a read
as much as a write, so that a read after a write in one transaction sees it — and
every `Read` and `Write` in that transaction goes through the session it
returned. When the DuckDB transaction resolves, Crossing calls `Commit` or
`Rollback` on it.

```cpp
class ToySession : public CrossingSession {
	CrossingScan Read(ClientContext &context, const CrossingQuery &query) override;
	CrossingWriter Write(ClientContext &context, const CrossingQuery &query) override;
	void Commit() override;
	void Rollback() override;

	ToyConnection conn;
};

unique_ptr<CrossingSession> ToyDB::Begin(ClientContext &context) {
	return make_uniq<ToySession>(ToyConnect());
}
```

One session per DuckDB transaction: two connections on one attach never share
one, which is where a connection of your own belongs. A source with no
transaction of its own leaves `Commit` and `Rollback` at their defaults, which
do nothing, and will not undo a partial write when the target rolls back.

A `Commit` that throws fails the statement that committed; the target has
already committed by then, so the error is a report, not an undo. A `Rollback`
that throws is swallowed.

### Detach

`Detach(context)` is called on `DETACH`, from the connection that ran it. The
source object itself is destroyed later, when DuckDB lets go of the catalog, so
anything that must happen on the user's connection — closing a session, telling
the other side — belongs here, not in the destructor. The default does nothing.

---

## Registering

```cpp
void ToyExtension::Load(ExtensionLoader &loader) {
	CrossingSource::Register(loader, "toydb", [](ClientContext &context, AttachInfo &info) {
		return make_uniq<ToyDB>(info.path);
	});
}
```

`"toydb"` is the name in `ATTACH ... (TYPE toydb)`. `info.path` is whatever was
attached — a host, a file, a connection string — and `info.options` carries the
rest of the parenthesised list, which is where a source that has to be authorised
rather than named reads its credentials. Your factory is called once per `ATTACH`,
and what it returns is the catalog's source for the life of the attach. The path
is yours alone: DuckDB never opens it as a database file.

Registering installs the storage extension for the type, the catalog's
transaction manager, and — once per database instance — the optimizer pass that
moves work across. This call is the only wiring; nothing else from crossing is
needed, and `crossing.hpp` is the only header to include.

The catalog this attaches holds the source's tables and nothing else: no native
DuckDB tables, no DDL, and no storage of its own. `CREATE`, `DROP` and `ALTER`
in it are refused with a message naming the source.

### Your own catalog

When that catalog is not the shape you need — native tables beside the source's,
a base class of your own, DDL forwarded somewhere — own the attach yourself.
`crossing_attach.hpp` gives `CrossingAttach`, one per `ATTACH`, which holds the
source, knows which tables it serves, hands out their catalog entries, and keeps
the source's side of every transaction. Your `Catalog`, from whatever base you
choose, holds one and forwards to it:

```cpp
class MyCatalog : public DuckCatalog {
	CrossingAttach attach;
	MyCatalog(AttachedDatabase &db, unique_ptr<CrossingSource> source) : DuckCatalog(db), attach(db, std::move(source)) {}
};

optional_ptr<CatalogEntry> MySchemaEntry::LookupEntry(CatalogTransaction, const EntryLookupInfo &lookup) {
	return attach.LookupTable(name, *this, lookup.GetEntryName());
}
```

The attach asks the source for `Schemas` and `Tables` the first time anything
needs them, not at construction. `LookupTable` and `ScanTables` describe a table
on first use and cache the entry for the life of the attach; `owner` is the
schema entry the table hangs off. `Tables`, `ServesTable`, `ThrowIfServed` and
`ThrowIfSchemaServed` are what a schema needs to answer `DROP` and `ALTER`.
`Described` is the table as the source described it, for anything that reports
on it. `Refresh()` forgets the listing and every description, so the next lookup
asks the source again; entries already handed out stay alive, since a binder may
still hold one. `Refresh(schema)` does the same for one schema's tables and
descriptions, keeping the schema list itself.

Your transaction manager calls `Release(transaction)` when a DuckDB transaction
ends, then `CrossingAttach::Commit` or `CrossingAttach::Rollback` on what comes
back. Release before a base class commits or rolls back: `DuckTransactionManager`
frees the `Transaction` object, and the attach keys on its address. A manager
that owns its transactions can call the instance `Commit(transaction)` and
`Rollback(transaction)` instead.

Your catalog's `OnDetach` calls `attach.Detach(context)` so the source hears it.

Your storage extension's `attach` builds the catalog; `create_transaction_manager`
builds the manager; then call `RegisterCrossingPass(db)` once, or nothing crosses.
`src/bridge/bridge_catalog.cpp` is a complete example on `DuckCatalog`.

### Two copies of crossing in one process

Two extensions that each compile crossing in can load side by side. Each copy
registers its own pass, keeps its own state, and recognises only the catalogs
its own `Register` created; a query that touches both crosses to each on its
own terms, and a join between them stays on the target. Both passes run on
every plan.

Crossing reaches into DuckDB's planner, which makes no ABI promise: build it
against the same DuckDB commit as the extension that carries it.

### Building it in

`crossing.cmake` gives `crossing_add_to_target(<target>)`, which adds the
sources and the include path to an extension target, or `crossing_sources` and
`crossing_includes` for a build that assembles its own list. Crossing depends on
DuckDB and nothing else in this repository; `src/vcat` is the bridge's and
provider's, not crossing's.

---

## What you get

`SELECT upper(name) FROM toy.orders WHERE amt > 100 LIMIT 10` gives you the
filter, the projection and the limit, over `orders` with your `USING` policy
beneath them. DuckDB runs nothing but the scan.

`WHERE amt > 100 AND md5(name) = '...'` gives you the `amt > 100` half only,
because `Accepts` said no to `md5`. DuckDB keeps the rest and filters what
you send back.

`toy.orders JOIN toy.customers` gives you one query over both tables, because
both are yours.

`UPDATE toy.orders SET amt = 0 WHERE id = 7` gives you an update query, keyed by
`id`, with your `USING` policy in its `WHERE` and your `WITH CHECK` guarding what
it writes.

`ORDER BY score, id LIMIT 100` gives you the sort and the limit together, and you
return a hundred rows rather than the whole table for DuckDB to sort. A sort
crosses on the same terms as everything else: `Accepts` is asked about each
sort key, and a key you refuse keeps the sort on the target.

The rows you hand back are then read in the order you produced them, because one
reader drains the scan.
