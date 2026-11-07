# Implementing a source

Crossing moves part of a DuckDB query onto another DuckDB. A source says what it has and moves frames between the two:

```sql
ATTACH 'toy.example.com' AS toy (TYPE toydb);
SELECT upper(name) FROM toy.orders WHERE amt > 100 LIMIT 10;
```

Near is the DuckDB attaching the source, far the DuckDB behind it. `crossing[a | B | c]` is one operator on near and ordinary pipelines `a | B | c` on far, with streams between them. DuckDB runs both sides: its threads, its ordering and its waiting. The source never sees a thread count.

## Near: the source

```cpp
class ToyDB : public CrossingSource {
public:
	vector<string> Tables(const string &schema) override;
	CrossingTable Describe(const string &schema, const string &name) override;
	unique_ptr<CrossingLink> Connect(ClientContext &context) override;
};

void ToyExtension::Load(ExtensionLoader &loader) {
	RegisterCrossing(loader, "toydb", [](ClientContext &, AttachInfo &info) -> unique_ptr<CrossingSource> {
		return make_uniq<ToyDB>(info.path);
	});
}
```

Optional members, with what happens if you leave them:

| member | default |
|---|---|
| `Schemas` | one schema, `main` |
| `DescribeSchema` | no schema allows `CREATE` |
| `AcceptsCall` | no function, operator, aggregate or window crosses; casts, comparisons and `BETWEEN` are not asked about |
| `AcceptsType` | no expression crosses except a bare reference to a described column |
| `AcceptsOperator` | every operator Substrait can carry crosses |
| `AcceptsSeam` | no join runs on far over rows near sends |
| `Group` | the source is its own group |
| `Detach` | nothing |
| `DescribeChanged` | `SELECT` and `DROP`, plus `INSERT`, `UPDATE`, `DELETE` and `ALTER` when far's table has a `rowid`, with `RETURNING` allowed; fills in a table this transaction changed, of which far has given the columns and its primary key, unique and not-null constraints. Remove any you do not declare in `Describe`, so a table describes the same before and after DDL |
| `Planned` | nothing; told of each slice as it is planned, for logging and tests |

`Describe` fills in `CrossingTable`: columns in source order, the verbs allowed, whether `RETURNING` is allowed, any constraints far enforces, and whether far's table has a `rowid` (a view has not). A verb left out is refused before the statement runs. Answers are cached per attach until `Refresh`.

Sources with the same `Group` serve one store through one link, so a plan may join their tables. They must answer the `Accepts*` calls alike. A source whose group spans catalogs sets `CrossingTable::catalog`.

## Moving frames: the link

```cpp
class ToyLink : public CrossingLink {
public:
	idx_t Streams() override;
	unique_ptr<CrossingStream> Open(idx_t id, optional_ptr<CrossingReceiver> receiver) override;
};

class ToyStream : public CrossingStream {
public:
	CrossingIo Send(CrossingFrame &frame, CrossingWaker waker) override;
	void Close(optional_ptr<const ErrorData> error) override;
};
```

- **A link is one far transaction.** `Connect` is called when a transaction first needs far. The link is destroyed after far has answered its commit or rollback. A read-only autocommit statement is the exception: far commits it itself, near waits for no reply, and a later statement in that near transaction connects a new link.
- **`Streams`** is how many streams a slice may use in each direction. It caps DuckDB's threads on that slice. Only near's end is asked; far uses the number near sends.
- **`Open(id, receiver)`** gives one end of a stream, and says where to hand what arrives. Both ends open the same id, in either order; hold what arrives before this end opens. `receiver` is null at an end that only sends. Streams are independent, so they can be connections, HTTP/2 streams or anything else.
- **`Send`** returns `TAKEN` once it no longer needs the frame, `WAIT` when it can't take it yet, or `DONE` when the other end closed without an error. On `WAIT`, call the waker when it may go on; crossing sends the same frame again.
- **`CrossingReceiver::Deliver`** takes each frame that arrives. Initialize `rows` from the receiver's `Types()` when the frame carries rows. It never waits on the transport, so one thread can deliver everything. `Closed` follows the last frame, with the error the other end closed with.
- **`Close`** ends this end, and nothing is delivered to it once `Close` returns. A non-null error reaches the other end: `Closed` gets it, and `Send` throws it.
- **`header`** is crossing's: carry it unread. **`rows`** may be empty, and has no columns on a frame that carries only a header. Encode rows however you like.

`Send` and `Close` are called from any thread, one thread at a time on each end of a stream. `Deliver` is called one at a time for each stream. `Open`, and `Wake` on a waker, may be called from any thread at any time. A waker may fire before the call that was given it returns.

## Far: serving the link

```cpp
Connection connection(db);
CrossingFar far(connection, "mycatalog");
far.Serve(link);
```

Crossing owns everything on far. `Serve` opens a transaction on `connection`, runs each statement near sends, and returns when near commits, rolls back or closes the link. Run one per link, on its own thread. It blocks. `Cancel` from any thread fails its statement, rolls back and makes it return.

`CrossingFarOptions` takes:

- `floors`: what stands for a table far reads, such as a subquery with a row filter. Its columns are the table's, in order, then `rowid`. An `UPDATE` or `DELETE` reaches only rows it produces, and fails with a clear error if it hides `rowid`. A `MERGE` into a table it replaces is refused.
- `catalogs`: called with each catalog a plan names; throw to refuse the plan.

## What travels

Every plan travels as Substrait, in frame headers. A statement's slices are one Substrait plan with one relation each, and crossing's own operators are extension relations: a seam is an `extensionLeaf`, a write an `extensionSingle`. Far binds them as ordinary statements under one plan on one executor, so slices never collide on the connection.

Whatever Substrait cannot express stays on near.

- **Rows** go through streams as they are produced, and land in an inbox DuckDB's scan reads, on either side. A seam takes every row and spills if it has to, so a sender never waits on far's results. Far's results are held back by what near has not yet read, by DuckDB's own rule for streaming results and its `streaming_buffer_size`. `VALUES` lists inside a slice still travel in the plan's JSON.
- **Writes** are native statements on far: `UPDATE ... FROM`, `DELETE ... USING`, `MERGE`, `INSERT`, and `CREATE TABLE AS`. When a write's shape cannot be split into target, source and condition, such as a `WHERE` with a subquery, its rows are addressed by far's `rowid`, which holds within the transaction. On a table declared with `row_ids = false`, such a write is refused at bind.
- **Order:** when row order matters, rows carry batch positions, and the receiving end releases them in order.
- **DDL** travels as SQL text, and a view's body as Substrait. Crossing asks far what a transaction sees of a schema it changed, then `DescribeChanged` fills in the rest.
- **Errors:** near does not wait for far to bind. Far closes the statement's streams with the error, keeping its type.

Writes on far run after the statement's reads have finished, and one at a time, so a transaction never scans a table while changing it.

## Transactions

A statement's failure on either side reaches the other: near closes its streams with the error, and far's errors arrive with their type. A commit that fails on far fails the statement. Nothing is undone elsewhere, so commit is not atomic across sources or between near and far.

## What is not supported

- The recursive part of a `WITH RECURSIVE` cannot read a source's table: its far plan runs once per statement, and recursion would need it to run again. This fails with an error.

## Your own catalog

`crossing_attach.hpp` gives `CrossingAttach`, one per `ATTACH`: it holds the source, lists and describes tables, hands out entries, and keeps each transaction's link.

```cpp
class MyCatalog : public DuckCatalog, public CrossingAttachOwner {
	CrossingAttach attach;
	MyCatalog(AttachedDatabase &db, unique_ptr<ToyDB> source) : DuckCatalog(db), attach(db, std::move(source)) {}
	CrossingAttach &Attach() override { return attach; }
};
```

- `LookupTable(schema, owner, name, transaction)`, `ScanTables`, `Described`: entries and descriptions, cached. `Refresh()` and `Refresh(schema)` forget them.
- `Ddl(context, transaction, owner, ddl)` checks the verb and runs it on far in the transaction. `DescribedSchema(schema)` is what the schema declares. `PlanCreateTableAs(...)` and `CreateView(context, transaction, owner, info)` are what a catalog's own methods forward to.
- `ServesTable`, `ThrowIfServed`, `ThrowIfSchemaServed` are for a catalog that refuses `DROP` or `ALTER`.
- `Release(transaction)` then `Commit` or `Rollback` on the result, before a base manager frees the `Transaction`; or `Commit(transaction)` and `Rollback(transaction)`.
- `OnDetach` calls `attach.Detach(context)`.
- Call `RegisterCrossingPass(db)`, or every read and write of a served table fails. Repeat calls do nothing. A far database needs `RegisterCrossingFarFunctions`, which `CrossingFar` calls itself.

`test/own_catalog/` is a complete example.

## Building

```cmake
crossing_add_to_target(toy_extension)
```

Adds the sources and include paths, including Substrait, which also needs DuckDB's `third_party/yyjson/include` on the include path and `libduckdb_yyjson` linked. `crossing_sources` and `crossing_includes` give the lists. A source includes `crossing.hpp` and `crossing_link.hpp`, and a far server `crossing_far.hpp`.

Two extensions each compiling crossing in coexist, each with its own identity. Build against the same DuckDB commit as the extension.
