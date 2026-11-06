#include "framework/twin.hpp"

#include "duckdb/main/query_result_stream.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

Value Scalar(Twin &twin, const string &far_sql) {
	auto result = twin.store->con.Query(far_sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return Collected(std::move(result))->GetValue(0, 0);
}

} // namespace

TEST_CASE("an insert of literal rows hands the source exactly those rows", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL), (7, NULL, 700, 3, NULL, "
	                         "'2024-01-07 00:00:00')");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	auto &call = twin.LastWrite();
	REQUIRE(call.kind == CrossingVerb::INSERT);
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.rows.size() == 2);
	REQUIRE(call.set_columns == vector<string> {"id", "name", "amt", "tenant", "score", "ts"});
	REQUIRE(call.tables.empty());
	REQUIRE(call.written.table == "orders");
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(7));
	twin.Same("SELECT * FROM far.orders ORDER BY id");
}

TEST_CASE("an insert fed by the same source runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders SELECT * FROM far.archive WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.Has(Op::LOGICAL_FILTER));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 9") == Value("zeta"));
}

TEST_CASE("an insert fed by the target hands the source the gathered rows", "[writes]") {
	Twin twin;
	twin.Seed();
	twin.Query("CREATE TABLE staged AS SELECT 8 AS id, 'eta' AS name, 800 AS amt, 3 AS tenant, 8.5 AS score, "
	           "TIMESTAMP '2024-01-08 00:00:00' AS ts");

	auto result = twin.Query("INSERT INTO far.orders SELECT * FROM staged");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.LastWrite().rows.size() == 1);
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 8") == Value("eta"));
}

TEST_CASE("an insert fed by another source gathers on the target", "[writes]") {
	Twin twin;
	twin.Seed();
	twin.Attach("second", "b");

	auto result = twin.Query("INSERT INTO far.orders SELECT * FROM b.archive");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.store->reads[0].tables[0].table == "archive");
}

TEST_CASE("an update runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 0, name = 'changed' WHERE id = 2");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.kind == CrossingVerb::UPDATE);
	REQUIRE(call.set_columns == vector<string> {"amt", "name"});
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(0));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 2") == Value("changed"));
	REQUIRE(Scalar(twin, "SELECT sum(amt) FROM orders") == Value::HUGEINT(1100));
}

TEST_CASE("an update on a table with no key reaches every row its condition does", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 0 WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(4));
	REQUIRE(Scalar(twin, "SELECT sum(amt) FROM orders") == Value::HUGEINT(50));
}

TEST_CASE("an update whose predicate far refuses addresses far's rows by row id", "[writes]") {
	Twin twin;
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 1 WHERE upper(name) = 'BETA'");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(!twin.store->reads[0].Mentions("upper"));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(1));
}

TEST_CASE("an update that sets a key column from another column lands", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = amt + tenant WHERE id IN (1, 2)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 1") == Value::INTEGER(51));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(151));
}

TEST_CASE("an update sets the values its FROM rows carry", "[writes]") {
	auto from = GENERATE(as<string> {}, "staged", "(SELECT 2 AS id, 'changed' AS name) staged",
	                     "(VALUES (2, 'changed')) staged(id, name)");
	INFO(from);
	Twin twin;
	twin.Seed();
	twin.Query("CREATE TABLE staged(id INTEGER, name VARCHAR)");
	twin.Query("INSERT INTO staged VALUES (2, 'changed')");

	auto result = twin.Query("UPDATE far.orders SET name = staged.name FROM " + from + " WHERE orders.id = staged.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 2") == Value("changed"));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 1") == Value("alpha"));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders WHERE name = 'changed'") == Value::BIGINT(1));
}

TEST_CASE("an update joined to a table the target holds hands the source only the matching rows", "[writes]") {
	Twin twin;
	twin.Seed();
	twin.Query("CREATE TABLE staged AS SELECT 2 AS id, 'changed' AS name");

	auto result = twin.Query("UPDATE far.orders SET name = staged.name FROM staged WHERE orders.id = staged.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.rows_only);
	REQUIRE(!call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.rows == vector<vector<Value>> {{Value::INTEGER(2), Value("changed")}});
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 2") == Value("changed"));
}

TEST_CASE("a delete joined to a table the target holds hands the source only the matching rows", "[writes]") {
	auto sql = GENERATE(as<string> {}, "DELETE FROM far.orders USING staged WHERE orders.id = staged.id",
	                    "DELETE FROM far.orders WHERE id IN (SELECT id FROM staged)");
	INFO(sql);
	Twin twin;
	twin.Seed();
	twin.Query("CREATE TABLE staged AS SELECT 2 AS id");

	auto result = twin.Query(sql);

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.rows_only);
	REQUIRE(!call.Has(Op::LOGICAL_FILTER));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(4));
}

TEST_CASE("an insert's work over rows the target holds stays on the target", "[writes]") {
	auto select = GENERATE(as<string> {}, "SELECT * FROM staged WHERE amt > 0",
	                       "SELECT id, upper(name), amt, tenant, score, ts FROM staged",
	                       "SELECT * FROM staged ORDER BY id LIMIT 1",
	                       "SELECT id, max(name), max(amt), max(tenant), max(score), max(ts) FROM staged GROUP BY id",
	                       "SELECT * FROM (VALUES (8, 'eta', 800, 3, 8.5, TIMESTAMP '2024-01-08 00:00:00')) "
	                       "v(id, name, amt, tenant, score, ts) WHERE amt > 0",
	                       "VALUES (8, 'eta', (random() * 0)::INTEGER + 800, 3, 8.5, NULL)");
	INFO(""
	     << " " << select);
	Twin twin;
	twin.Seed();
	twin.Query("CREATE TABLE staged AS SELECT 8 AS id, 'eta' AS name, 800 AS amt, 3 AS tenant, 8.5 AS score, "
	           "TIMESTAMP '2024-01-08 00:00:00' AS ts");

	auto result = twin.Query("INSERT INTO far.orders " + select);

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.rows_only);
	REQUIRE(!call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.rows.size() == 1);
	REQUIRE(call.tables.empty());
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 8") == Value::INTEGER(800));
}

TEST_CASE("an insert selecting nothing from the target's rows does not call the source", "[writes]") {
	auto select = GENERATE(as<string> {}, "SELECT * FROM staged WHERE false",
	                       "SELECT * FROM (VALUES (8, 'eta', 800, 3, 8.5, NULL)) v WHERE false");
	INFO(""
	     << " " << select);
	Twin twin;
	twin.Seed();
	twin.Query("CREATE TABLE staged AS SELECT 8 AS id, 'eta' AS name, 800 AS amt, 3 AS tenant, 8.5 AS score, "
	           "TIMESTAMP '2024-01-08 00:00:00' AS ts");

	auto result = twin.Query("INSERT INTO far.orders " + select);

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a function the source refuses still runs over the target's rows", "[writes]") {
	Twin twin;
	twin.store->refused_functions.insert("upper");
	twin.Seed();
	twin.Query("CREATE TABLE staged AS SELECT 8 AS id, 'eta' AS name, 800 AS amt, 3 AS tenant, 8.5 AS score, "
	           "TIMESTAMP '2024-01-08 00:00:00' AS ts");

	twin.Query("INSERT INTO far.orders SELECT id, upper(name), amt, tenant, score, ts FROM staged");

	auto &call = twin.LastWrite();
	REQUIRE(call.rows_only);
	REQUIRE(!call.Mentions("upper"));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 8") == Value("ETA"));
}

TEST_CASE("an update returning rows returns whole rows", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET name = 'changed' WHERE id = 2 RETURNING id, name, amt");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(2));
	REQUIRE(result->GetValue(1, 0) == Value("changed"));
	REQUIRE(result->GetValue(2, 0) == Value::INTEGER(150));
	auto &call = twin.LastWrite();
	REQUIRE(call.set_columns == vector<string> {"name"});
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 2") == Value("changed"));
}

TEST_CASE("a delete returning rows returns whole rows", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE id = 2 RETURNING id, name");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(2));
	REQUIRE(result->GetValue(1, 0) == Value("beta"));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(4));
}

TEST_CASE("a delete reports what the source removed", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(4));
	auto &call = twin.LastWrite();
	REQUIRE(call.kind == CrossingVerb::DELETE_);
	REQUIRE(call.set_columns.empty());
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(1));
}

TEST_CASE("a delete of nothing hands the source nothing and reports zero", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE amt > 10000");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a delete whose WHERE holds an IN over one source runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE id IN (SELECT tenant FROM far.customers)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_COMPARISON_JOIN));
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(2));
}

TEST_CASE("an update whose WHERE holds an IN over one source runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 0 WHERE id IN (SELECT tenant FROM far.customers)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_COMPARISON_JOIN));
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT sum(amt) FROM orders") == Value::HUGEINT(700));
}

TEST_CASE("a delete with an IN returns the rows it deleted", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE id IN (SELECT tenant FROM far.customers) RETURNING id");

	REQUIRE(result->RowCount() == 3);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(2));
}

TEST_CASE("an insert fed through an IN over one source runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query(
	    "INSERT INTO far.orders SELECT * FROM far.archive WHERE tenant NOT IN (SELECT tenant FROM far.customers)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_COMPARISON_JOIN));
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 9") == Value("zeta"));
}

TEST_CASE("a delete with a correlated EXISTS runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result =
	    twin.Query("DELETE FROM far.orders o WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant < o.tenant)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(!twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(2));
}

TEST_CASE("an update with a correlated EXISTS runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query(
	    "UPDATE far.orders o SET amt = 0 WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant < o.tenant)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(!twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT sum(amt) FROM orders") == Value::HUGEINT(200));
}

TEST_CASE("an insert fed by a CTE runs wholly on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders WITH x AS MATERIALIZED (SELECT * FROM far.archive) "
	                         "SELECT a.* FROM x a JOIN x e ON a.id = e.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(!twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 9") == Value("zeta"));
}

TEST_CASE("an insert fed by a CTE over a sample writes the rows both references read", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.archive WITH x AS MATERIALIZED (SELECT * FROM far.orders USING SAMPLE 2) "
	                         "SELECT a.* FROM x a JOIN x e ON a.id = e.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM archive") == Value::BIGINT(3));
}

TEST_CASE("an update reading no row changes every row", "[writes]") {
	Twin twin;
	twin.store->keys["dup"] = {"k"};
	twin.Far("CREATE TABLE dup(k INTEGER, v INTEGER)");
	twin.Far("INSERT INTO dup VALUES (1, 1), (1, 2), (2, 3)");
	twin.Attach();

	auto result = twin.Query("UPDATE far.dup SET v = 9");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	auto &call = twin.LastWrite();
	REQUIRE(call.set_columns == vector<string> {"v"});
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM dup WHERE v = 9") == Value::BIGINT(3));
}

TEST_CASE("a delete reading no row hands the source no seam", "[writes]") {
	Twin twin;
	twin.store->keys["dup"] = {"k"};
	twin.Far("CREATE TABLE dup(k INTEGER, v INTEGER)");
	twin.Far("INSERT INTO dup VALUES (1, 1), (1, 2), (2, 3)");
	twin.Attach();

	auto result = twin.Query("DELETE FROM far.dup");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	auto &call = twin.LastWrite();
	REQUIRE(call.types.empty());
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM dup") == Value::BIGINT(0));
}

TEST_CASE("a table granted update and delete without select takes only writes reading no row", "[writes]") {
	Twin twin;
	twin.store->verbs["orders"] = {CrossingVerb::UPDATE, CrossingVerb::DELETE_};
	twin.Seed();

	REQUIRE(twin.Query("UPDATE far.orders SET amt = 0")->GetValue(0, 0) == Value::BIGINT(5));
	for (auto &sql : {"UPDATE far.orders SET amt = amt + 1", "DELETE FROM far.orders WHERE id = 1",
	                  "DELETE FROM far.orders RETURNING id", "SELECT * FROM far.orders"}) {
		INFO(sql);
		REQUIRE_THAT(twin.Error(sql), Catch::Contains("does not have 'select' permission"));
	}
	REQUIRE(twin.Query("DELETE FROM far.orders")->GetValue(0, 0) == Value::BIGINT(5));
}

TEST_CASE("a merge changes what it matched", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("MERGE INTO far.orders t USING (SELECT 2 AS id UNION ALL SELECT 3) s ON t.id = s.id "
	                         "WHEN MATCHED THEN UPDATE SET amt = 0");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(0));
}

TEST_CASE("a merge matching one target row twice is refused and changes nothing", "[writes]") {
	Twin twin;
	twin.Seed();

	auto error = twin.Error("MERGE INTO far.orders t USING (SELECT 2 AS id UNION ALL SELECT 2) s ON t.id = s.id "
	                        "WHEN MATCHED THEN UPDATE SET amt = 0");

	REQUIRE_THAT(error, Catch::Contains("more than once"));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(150));
}

TEST_CASE("a merge lands its inserts and updates on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("MERGE INTO far.orders t USING (SELECT id, amt FROM far.archive) s ON t.id = s.id "
	                         "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'merged', s.amt, 0, NULL, NULL)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 9") == Value("merged"));
}

TEST_CASE("a merge with a failing action leaves nothing from its other action", "[writes]") {
	auto rows = GENERATE(as<string> {}, "SELECT 1 AS id, -1 AS v UNION ALL SELECT 2, 5",
	                     "SELECT 1 AS id, 5 AS v UNION ALL SELECT 2, -1");
	INFO(rows);
	Twin twin;
	twin.store->keys["guarded"] = {"id"};
	twin.Far("CREATE TABLE guarded(id INTEGER, v INTEGER CHECK (v >= 0))");
	twin.Far("INSERT INTO guarded VALUES (1, 10)");
	twin.Attach();

	auto error = twin.Error("MERGE INTO far.guarded t USING (" + rows +
	                        ") s ON t.id = s.id "
	                        "WHEN MATCHED THEN UPDATE SET v = s.v "
	                        "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)");

	REQUIRE_THAT(error, Catch::Contains("CHECK"));
	REQUIRE(twin.store->transaction_ends.back() == "rollback");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM guarded") == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT v FROM guarded WHERE id = 1") == Value::INTEGER(10));
}

TEST_CASE("a seam over the memory limit spills and lands whole", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE big(id BIGINT, v VARCHAR)");
	twin.Attach();
	twin.Query("SET memory_limit = '32MB'");

	auto result = twin.Query("INSERT INTO far.big SELECT i, repeat(md5(i::VARCHAR), 32) FROM range(64000) r(i)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(64000));
	REQUIRE(twin.LastWrite().rows_only);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM big WHERE v = repeat(md5(id::VARCHAR), 32)") == Value::BIGINT(64000));
}

TEST_CASE("a create table as over the memory limit spills and lands whole", "[writes]") {
	Twin twin;
	twin.store->schema_verbs["main"] = {CrossingVerb::CREATE};
	twin.Attach();
	twin.Query("SET memory_limit = '32MB'");

	twin.Query("CREATE TABLE far.big AS SELECT i AS id, repeat(md5(i::VARCHAR), 32) AS v FROM range(64000) r(i)");

	REQUIRE(Scalar(twin, "SELECT count(*) FROM big WHERE v = repeat(md5(id::VARCHAR), 32)") == Value::BIGINT(64000));
}

TEST_CASE("an insert hands the source only the columns it named, so the source's defaults fire", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7, w INTEGER DEFAULT 8)");
	twin.Attach();

	twin.Query("INSERT INTO far.d(id) VALUES (1)");
	REQUIRE(twin.LastWrite().set_columns == vector<string> {"id"});
	REQUIRE(twin.LastWrite().rows == vector<vector<Value>> {{Value::INTEGER(1)}});
	REQUIRE(Scalar(twin, "SELECT v FROM d WHERE id = 1") == Value::INTEGER(7));

	twin.Query("INSERT INTO far.d(w, id) VALUES (80, 2)");
	REQUIRE(twin.LastWrite().set_columns == vector<string> {"id", "w"});
	REQUIRE(Scalar(twin, "SELECT v FROM d WHERE id = 2") == Value::INTEGER(7));
	REQUIRE(Scalar(twin, "SELECT w FROM d WHERE id = 2") == Value::INTEGER(80));

	twin.Query("INSERT INTO far.d(id) SELECT i FROM range(3, 4) r(i)");
	REQUIRE(Scalar(twin, "SELECT w FROM d WHERE id = 3") == Value::INTEGER(8));
}

TEST_CASE("an insert over the source's own rows that names some columns still runs wholly on it", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7)");
	twin.Far("CREATE TABLE s(id INTEGER)");
	twin.Far("INSERT INTO s VALUES (1), (2)");
	twin.Attach();

	auto result = twin.Query("INSERT INTO far.d(id) SELECT id FROM far.s");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE_FALSE(twin.LastWrite().rows_only);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM d WHERE v = 7") == Value::BIGINT(2));
}

TEST_CASE("an insert of default values leaves every column to the source", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE d(id INTEGER DEFAULT 1, v INTEGER DEFAULT 7)");
	twin.Attach();

	auto result = twin.Query("INSERT INTO far.d DEFAULT VALUES");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.LastWrite().set_columns.empty());
	REQUIRE(Scalar(twin, "SELECT v FROM d WHERE id = 1") == Value::INTEGER(7));
}

TEST_CASE("a merge insert hands the source only the columns it named", "[writes]") {
	Twin twin;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7)");
	twin.Attach();

	twin.Query("MERGE INTO far.d t USING (SELECT 1 AS id) s ON t.id = s.id "
	           "WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)");

	REQUIRE(Scalar(twin, "SELECT v FROM d WHERE id = 1") == Value::INTEGER(7));
}

TEST_CASE("DEFAULT is refused rather than written as NULL", "[writes]") {
	auto sql =
	    GENERATE(as<string> {}, "INSERT INTO far.d VALUES (2, DEFAULT)", "INSERT INTO far.d(id, v) VALUES (2, DEFAULT)",
	             "UPDATE far.d SET v = DEFAULT", "UPDATE far.d SET v = DEFAULT WHERE id = 1",
	             "MERGE INTO far.d t USING (SELECT 1 AS id) s ON t.id = s.id "
	             "WHEN MATCHED THEN UPDATE SET v = DEFAULT",
	             "MERGE INTO far.d t USING (SELECT 2 AS id) s ON t.id = s.id "
	             "WHEN NOT MATCHED THEN INSERT VALUES (s.id, DEFAULT)");
	INFO(sql);
	Twin twin;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7)");
	twin.Far("INSERT INTO d VALUES (1, 10)");
	twin.Attach();

	auto error = twin.Error(sql);

	REQUIRE_THAT(error, Catch::Contains("only the source knows this column's default"));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM d") == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT v FROM d WHERE id = 1") == Value::INTEGER(10));
}

TEST_CASE("a conflict clause leaving out a column is refused and changes nothing", "[writes][on_conflict]") {
	auto sql = GENERATE(as<string> {}, "INSERT INTO far.d(id) VALUES (1) ON CONFLICT (id) DO UPDATE SET w = excluded.v",
	                    "INSERT INTO far.d(id) VALUES (2) ON CONFLICT (id) DO UPDATE SET w = 0",
	                    "INSERT INTO far.d(id) VALUES (2) ON CONFLICT DO NOTHING",
	                    "INSERT OR REPLACE INTO far.d(id) VALUES (1)",
	                    "INSERT INTO far.d VALUES (2, DEFAULT, 0) ON CONFLICT DO NOTHING");
	INFO(sql);
	Twin twin;
	twin.store->declare_key_constraints = true;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER PRIMARY KEY, v INTEGER DEFAULT 7, w INTEGER)");
	twin.Far("INSERT INTO d VALUES (1, 10, 20)");
	twin.Attach();

	auto error = twin.Error(sql);

	REQUIRE_THAT(error, Catch::Contains("must give every column a value"));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM d") == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT w FROM d WHERE id = 1") == Value::INTEGER(20));
}

TEST_CASE("a conflict clause that names every column reads excluded from the row it was given",
          "[writes][on_conflict]") {
	Twin twin;
	twin.store->declare_key_constraints = true;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER PRIMARY KEY, v INTEGER DEFAULT 7, w INTEGER)");
	twin.Far("INSERT INTO d VALUES (1, 10, 20)");
	twin.Attach();

	twin.Query("INSERT INTO far.d(id, v, w) VALUES (1, 7, 0) ON CONFLICT (id) DO UPDATE SET w = excluded.v");

	REQUIRE(Scalar(twin, "SELECT w FROM d WHERE id = 1") == Value::INTEGER(7));
}

TEST_CASE("DESCRIBE shows that a column's default belongs to the source", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE d(id INTEGER)");
	twin.Attach();

	auto result = twin.Query("DESCRIBE far.d");

	REQUIRE(result->GetValue(4, 0) == Value("crossing_source_default()"));
}

TEST_CASE("a local copy of a source's table takes inserts that leave a column out", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7)");
	twin.Far("INSERT INTO d VALUES (1, 10)");
	twin.Attach();
	twin.Query("ATTACH ':memory:' AS copied");
	twin.Query("COPY FROM DATABASE far TO copied");

	twin.Query("INSERT INTO copied.d(id) VALUES (2)");

	REQUIRE(twin.Query("SELECT v FROM copied.d WHERE id = 1")->GetValue(0, 0) == Value::INTEGER(10));
	REQUIRE(twin.Query("SELECT v FROM copied.d WHERE id = 2")->GetValue(0, 0).IsNull());
}

TEST_CASE("a write the source runs arrives as a node holding what it writes", "[writes]") {
	Twin twin;
	twin.Seed();

	twin.Query("UPDATE far.orders SET amt = 0 WHERE id = 1");
	REQUIRE(twin.LastWrite().kind == CrossingVerb::UPDATE);
	REQUIRE(twin.LastWrite().set_columns == vector<string> {"amt"});

	twin.Query("DELETE FROM far.orders");
	REQUIRE(twin.LastWrite().kind == CrossingVerb::DELETE_);
	REQUIRE(twin.LastWrite().operators.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(0));
}

TEST_CASE("RETURNING comes from the source, so its defaults and values show", "[writes][returning]") {
	Twin twin;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7, w INTEGER DEFAULT 8)");
	twin.Attach();

	auto inserted = twin.Query("INSERT INTO far.d(id) VALUES (1) RETURNING id, v, w");
	REQUIRE(inserted->RowCount() == 1);
	REQUIRE(inserted->GetValue(1, 0) == Value::INTEGER(7));
	REQUIRE(inserted->GetValue(2, 0) == Value::INTEGER(8));
	REQUIRE(twin.LastWrite().set_columns == vector<string> {"id"});
	REQUIRE(twin.LastWrite().returning_columns == vector<string> {"id", "v", "w"});

	auto narrowed = twin.Query("INSERT INTO far.d(id) VALUES (2) RETURNING w");
	REQUIRE(narrowed->ColumnCount() == 1);
	REQUIRE(narrowed->GetValue(0, 0) == Value::INTEGER(8));

	auto updated = twin.Query("UPDATE far.d SET v = 0 WHERE id = 1 RETURNING id");
	REQUIRE(updated->RowCount() == 1);
	REQUIRE(twin.LastWrite().set_columns == vector<string> {"v"});

	auto whole = twin.Query("UPDATE far.d SET v = v WHERE id = 1 RETURNING *");
	REQUIRE(whole->GetValue(1, 0) == Value::INTEGER(0));
	REQUIRE(whole->ColumnCount() == 3);

	auto gone = twin.Query("DELETE FROM far.d WHERE id = 2 RETURNING v, id");
	REQUIRE(gone->GetValue(0, 0) == Value::INTEGER(7));
	REQUIRE(gone->GetValue(1, 0) == Value::INTEGER(2));

	auto nothing = twin.Query("DELETE FROM far.d WHERE id = 99 RETURNING *");
	REQUIRE(nothing->RowCount() == 0);
}

TEST_CASE("a merge's RETURNING comes from the source", "[writes][returning]") {
	Twin twin;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER DEFAULT 7, w INTEGER DEFAULT 8)");
	twin.Far("INSERT INTO d VALUES (1, 10, 20)");
	twin.Attach();

	auto inserted = twin.Query("MERGE INTO far.d t USING (SELECT 2 AS id) s ON t.id = s.id "
	                           "WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id) RETURNING id, v, w");
	REQUIRE(inserted->RowCount() == 1);
	REQUIRE(inserted->GetValue(1, 0) == Value::INTEGER(7));
	REQUIRE(inserted->GetValue(2, 0) == Value::INTEGER(8));

	auto updated = twin.Query("MERGE INTO far.d t USING (SELECT 1 AS id, 0 AS v) s ON t.id = s.id "
	                          "WHEN MATCHED THEN UPDATE SET v = s.v RETURNING id, v, w");
	REQUIRE(updated->GetValue(1, 0) == Value::INTEGER(0));
	REQUIRE(updated->GetValue(2, 0) == Value::INTEGER(20));

	auto deleted = twin.Query("MERGE INTO far.d t USING (SELECT 1 AS id) s ON t.id = s.id "
	                          "WHEN MATCHED THEN DELETE RETURNING id, w");
	REQUIRE(deleted->GetValue(1, 0) == Value::INTEGER(20));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM d") == Value::BIGINT(1));

	auto nothing = twin.Query("MERGE INTO far.d t USING (SELECT 99 AS id) s ON t.id = s.id "
	                          "WHEN MATCHED THEN DELETE RETURNING id");
	REQUIRE(nothing->RowCount() == 0);
}

TEST_CASE("a merge's RETURNING is refused on a source that cannot return rows", "[writes][returning]") {
	Twin twin;
	twin.store->returning = false;
	twin.Seed();

	auto error = twin.Error("MERGE INTO far.orders t USING (SELECT 2 AS id) s ON t.id = s.id "
	                        "WHEN MATCHED THEN UPDATE SET amt = 0 RETURNING id");

	REQUIRE_THAT(error, Catch::Contains("cannot return rows"));
	REQUIRE(twin.store->writes.empty());
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(150));
}

TEST_CASE("a merge's seam over the memory limit spills and lands whole", "[writes]") {
	Twin twin;
	twin.store->keys["big"] = {"id"};
	twin.Far("CREATE TABLE big(id BIGINT, v VARCHAR)");
	twin.Attach();
	twin.Query("SET memory_limit = '32MB'");

	auto result = twin.Query("MERGE INTO far.big t USING (SELECT i AS id, repeat(md5(i::VARCHAR), 32) AS v "
	                         "FROM range(64000) r(i)) s ON t.id = s.id "
	                         "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(64000));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM big WHERE v = repeat(md5(id::VARCHAR), 32)") == Value::BIGINT(64000));
}

TEST_CASE("a conflict clause's RETURNING answers as plain DuckDB does", "[writes][returning][on_conflict]") {
	auto sql = GENERATE(as<string> {},
	                    "INSERT INTO far.d(id, v, w) VALUES (1, 5, 0), (2, 6, 0) ON CONFLICT (id) DO UPDATE "
	                    "SET v = excluded.v RETURNING id, v, w",
	                    "INSERT INTO far.d(id, v, w) VALUES (1, 5, 0), (2, 6, 0) ON CONFLICT DO NOTHING RETURNING *");
	INFO(sql);
	const string table = "CREATE TABLE d(id INTEGER PRIMARY KEY, v INTEGER, w INTEGER DEFAULT 8)";
	const string row = "INSERT INTO d VALUES (1, 10, 20)";

	Twin plain;
	plain.Far(table);
	plain.Far(row);
	auto expected = Collected(plain.store->con.Query(Twin::Unprefixed(sql)));
	REQUIRE(!expected->HasError());

	Twin twin;
	twin.store->declare_key_constraints = true;
	twin.store->keys["d"] = {"id"};
	twin.Far(table);
	twin.Far(row);
	twin.Attach();
	auto got = twin.Query(sql);

	REQUIRE(got->RowCount() == expected->RowCount());
	REQUIRE(got->ColumnCount() == expected->ColumnCount());
	set<string> got_rows;
	set<string> expected_rows;
	for (idx_t r = 0; r < got->RowCount(); r++) {
		string got_row;
		string expected_row;
		for (idx_t c = 0; c < got->ColumnCount(); c++) {
			got_row += got->GetValue(c, r).ToString() + "|";
			expected_row += expected->GetValue(c, r).ToString() + "|";
		}
		got_rows.insert(got_row);
		expected_rows.insert(expected_row);
	}
	REQUIRE(got_rows == expected_rows);
	const string rows = "SELECT string_agg(d::VARCHAR, ';' ORDER BY id) FROM d";
	REQUIRE(Scalar(twin, rows) == Scalar(plain, rows));
}

TEST_CASE("a prepared merge runs again", "[writes]") {
	Twin twin;
	twin.Seed();

	auto prepared = twin.con.Prepare("MERGE INTO far.orders t USING (SELECT ?::INTEGER AS id, ?::INTEGER AS amt) s "
	                                 "ON t.id = s.id WHEN MATCHED THEN UPDATE SET amt = s.amt "
	                                 "WHEN NOT MATCHED THEN INSERT (id, amt) VALUES (s.id, s.amt)");
	REQUIRE(!prepared->HasError());
	REQUIRE(!Collected(prepared->Execute(2, 1))->HasError());
	REQUIRE(!Collected(prepared->Execute(2, 3))->HasError());
	REQUIRE(!Collected(prepared->Execute(50, 4))->HasError());

	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(3));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 50") == Value::INTEGER(4));
}

TEST_CASE("RETURNING over local rows that select nothing makes no call", "[writes][returning]") {
	Twin twin;
	twin.Seed();

	auto result =
	    twin.Query("INSERT INTO far.orders SELECT i::INTEGER, 'p', 1, 1, 1, NULL FROM range(0) r(i) RETURNING id");

	REQUIRE(result->RowCount() == 0);
}

TEST_CASE("RETURNING on a source that cannot return rows is refused before it hears of it", "[writes][returning]") {
	auto sql = GENERATE(as<string> {}, "INSERT INTO far.orders VALUES (6, 'z', 1, 1, 1, NULL) RETURNING id",
	                    "UPDATE far.orders SET amt = 0 RETURNING id", "DELETE FROM far.orders RETURNING id");
	INFO(sql);
	Twin twin;
	twin.store->returning = false;
	twin.Seed();

	REQUIRE_THAT(twin.Error(sql), Catch::Contains("cannot return rows"));
	REQUIRE(twin.store->writes.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders WHERE amt = 0") == Value::BIGINT(0));
}

TEST_CASE("a source whose table changed refuses RETURNING with nothing written", "[writes][returning]") {
	auto change = GENERATE(as<string> {}, "ALTER TABLE d DROP COLUMN w", "ALTER TABLE d ALTER COLUMN w TYPE VARCHAR");
	INFO(""
	     << " " << change);
	Twin twin;
	twin.store->keys["d"] = {"id"};
	twin.Far("CREATE TABLE d(id INTEGER, v INTEGER, w INTEGER)");
	twin.Far("INSERT INTO d VALUES (1, 10, 20)");
	twin.Attach();
	twin.Query("SELECT * FROM far.d");
	twin.Far(change);

	twin.Error("UPDATE far.d SET v = 0 RETURNING *");

	REQUIRE(Scalar(twin, "SELECT v FROM d") == Value::INTEGER(10));
}

TEST_CASE("a result abandoned after one chunk leaves what plain DuckDB leaves", "[writes][returning]") {
	const string make = "CREATE TABLE big AS SELECT i::INTEGER AS id, 0 AS v FROM range(5000) r(i)";
	const string count = "SELECT count(*) FROM big WHERE v = 1";
	auto abandon = [](Connection &con, const string &sql) {
		auto result = con.Submit(sql);
		REQUIRE(!result->HasError());
		QueryResultStream<ChunkFormat> stream(std::move(result));
		REQUIRE(stream.Fetch());
		stream.Close();
	};

	Twin plain;
	plain.Far(make);
	abandon(plain.store->con, "UPDATE big SET v = 1 RETURNING id");
	auto plain_count = Scalar(plain, count);
	INFO("plain DuckDB leaves " << plain_count.ToString());

	Twin twin;
	twin.store->keys["big"] = {"id"};
	twin.Far(make);
	twin.Attach();
	abandon(twin.con, "UPDATE far.big SET v = 1 RETURNING id");

	REQUIRE(plain_count == Value::BIGINT(0));
	REQUIRE(Scalar(twin, count) == plain_count);
}

TEST_CASE("RETURNING rows over the memory limit spill and all come back", "[writes][returning]") {
	Twin twin;
	twin.store->keys["big"] = {"id"};
	twin.Far(
	    "CREATE TABLE big AS SELECT i::BIGINT AS id, repeat(md5(i::VARCHAR), 32) AS v, 0 AS w FROM range(24000) r(i)");
	twin.Attach();
	twin.Query("SET memory_limit = '16MB'");

	auto result = twin.con.Submit("UPDATE far.big SET w = 1 RETURNING id, v");
	REQUIRE(!result->HasError());
	QueryResultStream<ChunkFormat> stream(std::move(result));
	idx_t rows = 0;
	idx_t wrong = 0;
	while (auto chunk = stream.Fetch()) {
		for (idx_t r = 0; r < chunk->size(); r++) {
			rows++;
			wrong += chunk->GetValue(1, r).ToString().size() == 1024 ? 0 : 1;
		}
	}
	stream.Close();

	REQUIRE(rows == 24000);
	REQUIRE(wrong == 0);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM big WHERE w = 1") == Value::BIGINT(24000));
}

TEST_CASE("a write inside a WITH clause is refused and changes nothing", "[writes]") {
	auto sql = GENERATE(as<string> {}, "WITH x AS (DELETE FROM far.orders RETURNING id) SELECT 1",
	                    "WITH x AS (DELETE FROM far.orders WHERE id = 1 RETURNING id) SELECT * FROM x",
	                    "WITH x AS (UPDATE far.orders SET amt = 0 RETURNING id) SELECT count(*) FROM x",
	                    "WITH x AS (INSERT INTO far.orders VALUES (6, 'z', 1, 1, 1, NULL) RETURNING id) SELECT 1");
	INFO(sql);
	Twin twin;
	twin.Seed();

	REQUIRE_THAT(twin.Error(sql), Catch::Contains("inside a WITH clause"));
	REQUIRE(twin.store->writes.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a prepared write runs again", "[writes]") {
	Twin twin;
	twin.Seed();

	auto prepared = twin.con.Prepare("UPDATE far.orders SET amt = ? WHERE id = ? RETURNING amt");
	REQUIRE(!prepared->HasError());
	for (int32_t amount : {1, 2}) {
		auto result = Collected(prepared->Execute(amount, 3));
		REQUIRE(!result->HasError());
		REQUIRE(result->GetValue(0, 0) == Value::INTEGER(amount));
		REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 3") == Value::INTEGER(amount));
	}
	auto insert = twin.con.Prepare("INSERT INTO far.orders SELECT i, 'p', 1, 1, 1, NULL FROM range(?, ?) r(i)");
	REQUIRE(!insert->HasError());
	REQUIRE(!Collected(insert->Execute(10, 12))->HasError());
	REQUIRE(!Collected(insert->Execute(20, 23))->HasError());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(10));
}

TEST_CASE("an insert keeps the order of its feed", "[writes]") {
	Twin twin;
	twin.Far("CREATE TABLE seq(id INTEGER)");
	twin.Attach();

	twin.Query("INSERT INTO far.seq SELECT i::INTEGER FROM range(5000) r(i) ORDER BY i DESC");

	auto &rows = twin.LastWrite().rows;
	REQUIRE(rows.size() == 5000);
	for (idx_t r = 0; r < rows.size(); r++) {
		if (rows[r][0] != Value::INTEGER(NumericCast<int32_t>(4999 - r))) {
			FAIL("row " << r << " out of order");
		}
	}
}

TEST_CASE("a write returning rows returns what the source now holds", "[writes]") {
	Twin twin;
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL) RETURNING id, name");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(6));
	REQUIRE(result->GetValue(1, 0) == Value("zeta"));
}

TEST_CASE("a verb the source did not grant is refused at bind time and never reaches the source", "[writes]") {
	Twin twin;
	twin.store->verbs["orders"] = {CrossingVerb::SELECT, CrossingVerb::INSERT};
	twin.Seed();

	auto error = twin.Error("DELETE FROM far.orders WHERE id = 1");

	REQUIRE_THAT(error, Catch::Contains("'orders' does not have 'delete' permission"));
	REQUIRE(twin.store->writes.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a rolled back transaction leaves the source as it was", "[writes]") {
	Twin twin;
	twin.Seed();

	twin.Query("BEGIN");
	twin.Query("UPDATE far.orders SET amt = 0 WHERE id = 2");
	twin.Query("DELETE FROM far.orders WHERE id = 3");
	twin.Query("ROLLBACK");

	REQUIRE(twin.store->transaction_ends == vector<string> {"rollback"});
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(150));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a committed transaction lands on the source as one change", "[writes]") {
	Twin twin;
	twin.Seed();

	twin.Query("BEGIN");
	twin.Query("UPDATE far.orders SET amt = 0 WHERE id = 2");
	twin.Query("DELETE FROM far.orders WHERE id = 3");
	REQUIRE(twin.store->transaction_ends.empty());
	twin.Query("COMMIT");

	REQUIRE(twin.store->transaction_ends == vector<string> {"commit"});
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(0));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(4));
}

TEST_CASE("a read after a write in one transaction sees the write", "[writes]") {
	Twin twin;
	twin.Seed();

	twin.Query("BEGIN");
	twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL)");
	auto during = twin.Query("SELECT count(*) FROM far.orders");
	twin.Query("COMMIT");

	REQUIRE(during->GetValue(0, 0) == Value::BIGINT(6));
	REQUIRE(twin.store->links_connected == 1);
}

TEST_CASE("each statement outside a transaction is its own session on the source", "[writes]") {
	Twin twin;
	twin.Seed();

	twin.Query("SELECT count(*) FROM far.orders");
	twin.Query("SELECT count(*) FROM far.orders");

	REQUIRE(twin.store->transaction_ends == vector<string> {"commit", "commit"});
}

TEST_CASE("a conflict clause on an undeclared key is refused and changes nothing", "[writes][on_conflict]") {
	auto sql =
	    GENERATE(as<string> {}, "INSERT INTO far.orders (id, amt) VALUES (1, 0) ON CONFLICT DO NOTHING",
	             "INSERT INTO far.orders (id, amt) VALUES (1, 0) ON CONFLICT (id) DO NOTHING",
	             "INSERT OR IGNORE INTO far.orders (id, amt) VALUES (1, 0)",
	             "INSERT OR REPLACE INTO far.orders (id, amt) VALUES (1, 0)",
	             "INSERT INTO far.orders (id, amt) VALUES (1, 0) ON CONFLICT (id) DO UPDATE SET amt = excluded.amt");
	INFO(sql);
	Twin twin;
	twin.Seed();

	REQUIRE_THAT(twin.Error(sql), Catch::Contains("UNIQUE/PRIMARY KEY"));

	REQUIRE(twin.store->writes.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 1") == Value::INTEGER(50));
}

TEST_CASE("a conflict clause on a declared key leaves the rows the source itself would", "[writes][on_conflict]") {
	const string into = "INTO far.orders (id, name, amt, tenant, score, ts) VALUES ";
	const string row_1 = "(1, 'one', 0, 1, 1.0, NULL)";
	const string row_1_7 = "(1, 'one', 7, 1, 1.0, NULL)";
	const string row_6 = "(6, 'six', 600, 1, 6.0, NULL)";
	auto sql = GENERATE_COPY(
	    "INSERT " + into + row_1 + " ON CONFLICT DO NOTHING", "INSERT " + into + row_1 + " ON CONFLICT (id) DO NOTHING",
	    "INSERT OR IGNORE " + into + row_1, "INSERT OR REPLACE " + into + row_1,
	    "INSERT " + into + row_1 + " ON CONFLICT (id) DO UPDATE SET amt = excluded.amt",
	    "INSERT " + into + row_1_7 + " ON CONFLICT (id) DO UPDATE SET amt = orders.amt + excluded.amt",
	    "INSERT " + into + row_1 + " ON CONFLICT (id) DO UPDATE SET amt = excluded.amt WHERE orders.amt > 1000",
	    "INSERT " + into + row_1 + ", " + row_6 + " ON CONFLICT (id) DO UPDATE SET amt = excluded.amt",
	    "INSERT " + into + row_1 + ", " + row_6 + " ON CONFLICT DO NOTHING");
	INFO(sql);
	const string rows = "SELECT string_agg(o::VARCHAR, ';' ORDER BY id) FROM orders o";

	Twin plain;
	plain.Seed();
	plain.Far(Twin::Unprefixed(sql));

	Twin twin;
	twin.store->declare_key_constraints = true;
	twin.Seed();
	twin.Query(sql);

	REQUIRE(Scalar(twin, rows) == Scalar(plain, rows));
}
