#include "framework/twin.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

using namespace duckdb;

namespace {

Value Scalar(Twin &twin, const string &sql) {
	auto result = twin.store->con.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return result->Fetch()->GetValue(0, 0);
}

struct NoKeyTable {
	explicit NoKeyTable(idx_t rows = 5) : twin() {
		twin.Far("CREATE TABLE plain(a INTEGER, b INTEGER)");
		twin.Far("INSERT INTO plain SELECT i::INTEGER, 0 FROM range(" + to_string(rows) + ") r(i)");
		twin.Far("CREATE TABLE other(a INTEGER, c INTEGER)");
		twin.Far("INSERT INTO other VALUES (1, 11), (2, 22)");
		twin.Attach();
	}
	Twin twin;
};

} // namespace

TEST_CASE("an update with no key runs on far and sends near no rows", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("UPDATE far.plain SET b = a + 1 WHERE a > 2");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(9));
}

TEST_CASE("a delete with no key runs on far and sends near no rows", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("DELETE FROM far.plain WHERE a < 2");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain") == Value::BIGINT(3));
}

TEST_CASE("an update from another far table crosses whole", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("UPDATE far.plain SET b = o.c FROM far.other o WHERE plain.a = o.a");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(33));
}

TEST_CASE("an update from near rows sends only those rows", "[shaped]") {
	NoKeyTable t;
	t.twin.Query("CREATE TABLE here(a INTEGER, v INTEGER)");
	t.twin.Query("INSERT INTO here VALUES (0, 5), (4, 6)");
	auto result = t.twin.Query("UPDATE far.plain SET b = h.v FROM here h WHERE plain.a = h.a");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->seam_rows == 2);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(11));
}

TEST_CASE("a merge reading far runs on far as one operator", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("MERGE INTO far.plain t USING far.other s ON t.a = s.a "
	                           "WHEN MATCHED THEN UPDATE SET b = s.c");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(33));
}

TEST_CASE("a merge from near rows sends only those rows, and all its actions land", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("MERGE INTO far.plain t USING (VALUES (0, 9), (9, 9)) s(a, c) ON t.a = s.a "
	                           "WHEN MATCHED THEN UPDATE SET b = s.c "
	                           "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.c)");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->seam_rows == 2);
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain") == Value::BIGINT(6));
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(18));
}

TEST_CASE("a merge returns its rows and which action made them", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("MERGE INTO far.plain t USING far.other s ON t.a = s.a "
	                           "WHEN MATCHED THEN UPDATE SET b = s.c RETURNING a, b, merge_action");
	REQUIRE(result->RowCount() == 2);
	auto first = result->GetValue(0, 0).GetValue<int32_t>();
	REQUIRE(result->GetValue(1, 0) == Value::INTEGER(first * 11));
	REQUIRE(result->GetValue(2, 1).ToString() == "UPDATE");
}

TEST_CASE("a table scanned while written in one statement reads as it was", "[shaped]") {
	NoKeyTable t(1000);
	auto result =
	    t.twin.Query("UPDATE far.plain SET a = a + 1 FROM (SELECT max(a) AS m FROM far.plain) x WHERE plain.a < x.m");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(999));
	REQUIRE(Scalar(t.twin, "SELECT max(a) FROM plain") == Value::INTEGER(999));
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain WHERE a = 999") == Value::BIGINT(2));
}

TEST_CASE("an update reaching two rows with one value changes both, as DuckDB does", "[shaped]") {
	NoKeyTable t;
	t.twin.Far("CREATE TABLE dup(k INTEGER, v INTEGER)");
	t.twin.Far("INSERT INTO dup VALUES (1, 0), (1, 0), (2, 0)");
	t.twin.store->verbs.clear();
	t.twin.Query("DETACH far");
	t.twin.Attach();

	auto result = t.twin.Query("UPDATE far.dup SET v = 9 WHERE k = 1 RETURNING k");
	REQUIRE(result->RowCount() == 2);
	REQUIRE(Scalar(t.twin, "SELECT sum(v) FROM dup") == Value::HUGEINT(18));
}

TEST_CASE("a delete whose condition is a subquery addresses far's rows by row id", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("DELETE FROM far.plain WHERE a IN (SELECT a FROM far.other)");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain") == Value::BIGINT(3));
}

TEST_CASE("a delete filtered by a function far refuses sends near's row ids back", "[shaped]") {
	Twin twin;
	twin.store->refused_functions.insert("upper");
	twin.Far("CREATE TABLE s(a VARCHAR)");
	twin.Far("INSERT INTO s VALUES ('x'), ('y'), ('z')");
	twin.Attach();

	auto result = twin.Query("DELETE FROM far.s WHERE upper(a) = 'Y'");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.store->seam_rows == 1);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM s") == Value::BIGINT(2));
}

TEST_CASE("many streams and threads give the same answers through an attach", "[link]") {
	auto [streams, near_threads] = GENERATE(std::make_pair<idx_t, idx_t>(1, 1), std::make_pair<idx_t, idx_t>(4, 4),
	                                        std::make_pair<idx_t, idx_t>(4, 1), std::make_pair<idx_t, idx_t>(2, 4));
	INFO(streams << " streams, " << near_threads << " threads");
	Twin twin;
	twin.store->streams = streams;
	twin.store->capacity = 1;
	twin.store->takes_seams = true;
	twin.Far("CREATE TABLE big AS SELECT i::BIGINT AS id, (i % 7)::BIGINT AS v FROM range(50000) r(i)");
	twin.Attach();
	twin.Query("SET threads = " + to_string(near_threads));
	twin.Query("CREATE TABLE loc AS SELECT i::BIGINT AS id FROM range(0, 50000, 5) r(i)");

	auto ordered = twin.Query("SELECT id FROM far.big ORDER BY id DESC");
	REQUIRE(ordered->RowCount() == 50000);
	for (idx_t r = 0; r < 50000; r += 997) {
		REQUIRE(ordered->GetValue(0, r) == Value::BIGINT(NumericCast<int64_t>(49999 - r)));
	}
	auto joined = twin.Query("SELECT count(*), sum(b.v) FROM far.big b JOIN loc l ON b.id = l.id");
	REQUIRE(joined->GetValue(0, 0) == Value::BIGINT(10000));

	twin.Query("CREATE TABLE far.main.copy AS SELECT id FROM far.big ORDER BY id");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM copy") == Value::BIGINT(50000));
	twin.Query("INSERT INTO far.main.copy SELECT id FROM loc");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM copy") == Value::BIGINT(60000));
	auto limited = twin.Query("SELECT id FROM far.big LIMIT 3");
	REQUIRE(limited->RowCount() == 3);
}

TEST_CASE("an insert into a source keeps an ordered feed's order across streams", "[link]") {
	Twin twin;
	twin.store->streams = 4;
	twin.Far("CREATE TABLE seq(id BIGINT)");
	twin.Attach();
	twin.Query("SET threads = 4");

	twin.Query("INSERT INTO far.seq SELECT i::BIGINT FROM range(100000) r(i) ORDER BY i DESC");

	auto rows = twin.store->con.Query("SELECT id FROM seq");
	idx_t r = 0;
	while (auto chunk = rows->Fetch()) {
		for (idx_t c = 0; c < chunk->size(); c++, r++) {
			if (chunk->GetValue(0, c) != Value::BIGINT(NumericCast<int64_t>(99999 - r))) {
				FAIL("row " << r << " out of order");
			}
		}
	}
	REQUIRE(r == 100000);
}

TEST_CASE("a failure rolls back an explicit transaction and leaves the attach usable", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE c(id INTEGER CHECK (id < 100))");
	twin.Attach();

	twin.Query("BEGIN");
	twin.Query("INSERT INTO far.c VALUES (1)");
	REQUIRE_THAT(twin.Error("INSERT INTO far.c SELECT i::INTEGER FROM range(1000) r(i)"), Catch::Contains("CHECK"));
	REQUIRE_THAT(twin.Error("SELECT * FROM far.c"), Catch::Contains("aborted"));
	twin.Query("ROLLBACK");
	REQUIRE(twin.store->transaction_ends.back() == "rollback");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM c") == Value::BIGINT(0));

	twin.Query("INSERT INTO far.c VALUES (2)");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM c") == Value::BIGINT(1));
	REQUIRE(twin.store->transaction_ends.back() == "commit");
}

TEST_CASE("a feed that fails mid-statement ends far's side of it", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE c(id INTEGER)");
	twin.Attach();

	REQUIRE_THAT(twin.Error("INSERT INTO far.c SELECT CASE WHEN i = 50000 THEN error('boom') ELSE i::INTEGER END FROM "
	                        "range(100000) r(i)"),
	             Catch::Contains("boom"));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM c") == Value::BIGINT(0));
	twin.Query("INSERT INTO far.c VALUES (1)");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM c") == Value::BIGINT(1));
}

TEST_CASE("two statements in one transaction see each other's writes on far", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE c(id INTEGER)");
	twin.Attach();

	twin.Query("BEGIN");
	twin.Query("INSERT INTO far.c VALUES (1), (2)");
	REQUIRE(twin.Query("SELECT count(*) FROM far.c")->GetValue(0, 0) == Value::BIGINT(2));
	twin.Query("UPDATE far.c SET id = id + 10");
	REQUIRE(twin.Query("SELECT sum(id) FROM far.c")->GetValue(0, 0) == Value::HUGEINT(23));
	twin.Query("COMMIT");
	REQUIRE(Scalar(twin, "SELECT sum(id) FROM c") == Value::HUGEINT(23));
}

TEST_CASE("far's floor resolver applies a row policy to reads and writes", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE p(id INTEGER, tenant INTEGER)");
	twin.Far("INSERT INTO p VALUES (1, 1), (2, 1), (3, 2)");
	auto &far_context = *twin.store->con.context;
	twin.store->floors = [&far_context](const CrossingFloor &floor) -> unique_ptr<TableRef> {
		Parser parser(far_context);
		parser.ParseQuery("SELECT *, rowid FROM memory.main." + floor.table + " WHERE tenant = 1");
		auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
		return make_uniq<SubqueryRef>(std::move(select), Identifier(floor.table));
	};
	twin.Attach();

	REQUIRE(twin.Query("SELECT count(*) FROM far.p")->GetValue(0, 0) == Value::BIGINT(2));

	REQUIRE(twin.Query("UPDATE far.p SET id = 0")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM p WHERE id = 0") == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT id FROM p WHERE tenant = 2") == Value::INTEGER(3));

	REQUIRE(twin.Query("DELETE FROM far.p WHERE id = 3 OR id = 0")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM p WHERE tenant = 2") == Value::BIGINT(1));

	REQUIRE_THAT(twin.Error("MERGE INTO far.p t USING (SELECT 3 AS id) s ON t.id = s.id WHEN MATCHED THEN DELETE"),
	             Catch::Contains("row policy"));
}

TEST_CASE("far uses the stream count near sends, whatever its own end reports", "[link]") {
	auto far_streams = GENERATE(idx_t(1), idx_t(8));
	INFO("far's end reports " << far_streams);
	Twin twin;
	twin.store->streams = 3;
	twin.store->far_streams = far_streams;
	twin.Far("CREATE TABLE big AS SELECT i::BIGINT AS id FROM range(50000) r(i)");
	twin.Attach();
	twin.Query("SET threads = 4");
	twin.Query("CREATE TABLE here AS SELECT i::BIGINT AS id FROM range(1000) r(i)");

	REQUIRE(twin.Query("SELECT count(*) FROM far.big")->GetValue(0, 0) == Value::BIGINT(50000));
	twin.Query("INSERT INTO far.big SELECT id FROM here");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM big") == Value::BIGINT(51000));
}

TEST_CASE("a read-only autocommit statement waits for no control reply", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE t(id INTEGER)");
	twin.Far("INSERT INTO t VALUES (1), (2)");
	twin.Attach();

	REQUIRE(twin.Query("SELECT count(*) FROM far.t")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(twin.store->near_control_replies == 0);
	REQUIRE(twin.store->transaction_ends.back() == "commit");

	twin.Query("BEGIN");
	REQUIRE(twin.Query("SELECT count(*) FROM far.t")->GetValue(0, 0) == Value::BIGINT(2));
	twin.Query("COMMIT");
	REQUIRE(twin.store->near_control_replies == 2);
	twin.Query("INSERT INTO far.t VALUES (3)");
	REQUIRE(twin.store->near_control_replies == 4);
}

TEST_CASE("a read far cannot bind fails with its error type, with no reply to wait for", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE t(id INTEGER)");
	twin.Attach();
	twin.Query("SELECT * FROM far.t");
	twin.Far("ALTER TABLE t ADD COLUMN extra INTEGER");

	REQUIRE_THAT(twin.Error("SELECT * FROM far.t"), Catch::Contains("changed on the source"));
	twin.Query("DETACH far");
	twin.Attach();
	REQUIRE(twin.Query("SELECT count(*) FROM far.t")->GetValue(0, 0) == Value::BIGINT(0));
}

TEST_CASE("a transaction reads the schema it changed through far alone", "[link]") {
	Twin twin;
	twin.Attach();

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.n(a INTEGER, s VARCHAR, l INTEGER[], st STRUCT(x INTEGER, y VARCHAR))");
	twin.Query("INSERT INTO far.n(a) VALUES (1)");
	auto read = twin.Query("SELECT a, s FROM far.n");
	REQUIRE(read->GetValue(0, 0) == Value::INTEGER(1));
	auto listed = twin.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far' AND table_name = 'n'");
	REQUIRE(listed->GetValue(0, 0) == Value::BIGINT(1));
	twin.Query("COMMIT");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM n") == Value::BIGINT(1));
}

TEST_CASE("a write a view's lack of row ids cannot address is refused at bind", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE t(id INTEGER)");
	twin.Far("CREATE VIEW v AS SELECT id FROM t");
	twin.store->verbs["v"] = {CrossingVerb::SELECT, CrossingVerb::DELETE_};
	twin.Attach();

	REQUIRE_THAT(twin.Error("DELETE FROM far.v WHERE id IN (SELECT id FROM far.t)"), Catch::Contains("has no row ids"));
}

TEST_CASE("a row policy hiding rowid reads, and refuses a write clearly", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE p(id INTEGER, tenant INTEGER)");
	twin.Far("INSERT INTO p VALUES (1, 1), (2, 2)");
	auto &far_context = *twin.store->con.context;
	twin.store->floors = [&far_context](const CrossingFloor &floor) -> unique_ptr<TableRef> {
		Parser parser(far_context);
		parser.ParseQuery("SELECT * FROM memory.main." + floor.table + " WHERE tenant = 1");
		auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
		return make_uniq<SubqueryRef>(std::move(select), Identifier(floor.table));
	};
	twin.Attach();

	REQUIRE(twin.Query("SELECT count(*) FROM far.p")->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE_THAT(twin.Error("UPDATE far.p SET id = 0"), Catch::Contains("hides rowid"));
}

TEST_CASE("a table keeps its constraints after DDL in its schema", "[link]") {
	Twin twin;
	twin.store->declare_key_constraints = true;
	twin.Far("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
	twin.Far("INSERT INTO t VALUES (1, 10)");
	twin.Attach();

	auto described = [&twin]() {
		auto result = twin.Query("DESCRIBE far.t");
		string text;
		for (idx_t r = 0; r < result->RowCount(); r++) {
			for (idx_t c = 0; c < result->ColumnCount(); c++) {
				text += result->GetValue(c, r).ToString() + "|";
			}
			text += "\n";
		}
		return text;
	};
	auto before = described();
	REQUIRE_THAT(before, Catch::Contains("PRI"));

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.other(a INTEGER)");

	REQUIRE(described() == before);
	twin.Query("INSERT INTO far.t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = excluded.v");
	twin.Query("COMMIT");
	REQUIRE(Scalar(twin, "SELECT v FROM t WHERE id = 1") == Value::INTEGER(20));
}

TEST_CASE("far reports a changed table's primary key, unique and not-null constraints", "[link]") {
	Twin twin;
	twin.store->keep_far_constraints = true;
	twin.Far("CREATE TABLE t(id INTEGER PRIMARY KEY, u INTEGER UNIQUE, n INTEGER NOT NULL, plain INTEGER)");
	twin.Attach();

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.other(a INTEGER)");
	auto result = twin.Query("DESCRIBE far.t");
	twin.Query("COMMIT");

	REQUIRE(result->GetValue(0, 0).ToString() == "id");
	REQUIRE(result->GetValue(3, 0).ToString() == "PRI");
	REQUIRE(result->GetValue(2, 0).ToString() == "NO");
	REQUIRE(result->GetValue(3, 1).ToString() == "UNI");
	REQUIRE(result->GetValue(2, 2).ToString() == "NO");
	REQUIRE(result->GetValue(2, 3).ToString() == "YES");
}

TEST_CASE("a source removes a constraint it does not declare from a changed table", "[link]") {
	Twin twin;
	twin.Far("CREATE TABLE t(id INTEGER PRIMARY KEY)");
	twin.Attach();

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.other(a INTEGER)");
	auto result = twin.Query("DESCRIBE far.t");
	twin.Query("COMMIT");

	REQUIRE(result->GetValue(3, 0).IsNull());
}

TEST_CASE("a table created in the transaction keeps the constraints it was created with", "[link]") {
	Twin twin;
	twin.store->keep_far_constraints = true;
	twin.Attach();

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.n(id INTEGER PRIMARY KEY, v INTEGER)");
	twin.Query("INSERT INTO far.n VALUES (1, 1)");
	twin.Query("INSERT INTO far.n VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v");
	twin.Query("COMMIT");

	REQUIRE(Scalar(twin, "SELECT v FROM n WHERE id = 1") == Value::INTEGER(2));
}
