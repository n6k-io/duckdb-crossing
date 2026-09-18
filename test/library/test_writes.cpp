#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

Value Scalar(Twin &twin, const string &far_sql) {
	auto result = twin.store->con.Query(far_sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return result->GetValue(0, 0);
}

} // namespace

TEST_CASE("an insert of literal rows hands the source exactly those rows", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL), (7, NULL, 700, 3, NULL, "
	                         "'2024-01-07 00:00:00')");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	auto &call = twin.LastWrite();
	REQUIRE(call.kind == CrossingVerb::INSERT);
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.rows.size() == 2);
	REQUIRE(call.set_columns == vector<string> {"id", "name", "amt", "tenant", "score", "ts"});
	REQUIRE(call.key_columns.empty());
	REQUIRE(call.tables.size() == 1);
	REQUIRE(call.tables[0].table == "orders");
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(7));
	twin.Same("SELECT * FROM far.orders ORDER BY id");
}

TEST_CASE("an insert fed by the same source runs wholly on the source", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
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
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
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
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.Attach("second", "b");

	auto result = twin.Query("INSERT INTO far.orders SELECT * FROM b.archive");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.store->reads[0].tables[0].table == "archive");
}

TEST_CASE("an update on a unique key runs wholly on the source", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 0, name = 'changed' WHERE id = 2");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.kind == CrossingVerb::UPDATE);
	REQUIRE(call.key_columns == vector<string> {"id"});
	REQUIRE(call.set_columns == vector<string> {"amt", "name"});
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.Has(Op::LOGICAL_FILTER));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(call.rows == vector<vector<Value>> {{Value::INTEGER(2), Value::INTEGER(0), Value("changed")}});
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(0));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 2") == Value("changed"));
	REQUIRE(Scalar(twin, "SELECT sum(amt) FROM orders") == Value::HUGEINT(1100));
}

TEST_CASE("an update on a key the source does not vouch for is fed its keys by the target", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->key_unique["orders"] = false;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 0 WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(4));
	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.rows.size() == 4);
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.store->reads[0].Has(Op::LOGICAL_FILTER));
	REQUIRE(Scalar(twin, "SELECT sum(amt) FROM orders") == Value::HUGEINT(50));
}

TEST_CASE("an update whose predicate the source cannot compute is fed its keys by the target", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = 1 WHERE upper(name) = 'BETA'");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.rows == vector<vector<Value>> {{Value::INTEGER(2), Value::INTEGER(1)}});
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(!twin.store->reads[0].Mentions("upper"));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(1));
}

TEST_CASE("an update that sets a key column from another column lands", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = amt + tenant WHERE id IN (1, 2)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 1") == Value::INTEGER(51));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(151));
}

TEST_CASE("a delete addresses rows by the key and reports what the source removed", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(4));
	auto &call = twin.LastWrite();
	REQUIRE(call.kind == CrossingVerb::DELETE_);
	REQUIRE(call.key_columns == vector<string> {"id"});
	REQUIRE(call.set_columns.empty());
	REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(1));
}

TEST_CASE("a delete of nothing hands the source nothing and reports zero", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE amt > 10000");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a key that identifies more than one source row fails the write and rolls it back", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->keys["dup"] = {"k"};
	twin.store->key_unique["dup"] = false;
	twin.Far("CREATE TABLE dup(k INTEGER, v INTEGER)");
	twin.Far("INSERT INTO dup VALUES (1, 1), (1, 2), (2, 3)");
	twin.Attach();

	auto error = twin.Error("UPDATE far.dup SET v = 9 WHERE k = 1");

	REQUIRE_THAT(error, Catch::Contains("the declared key (k) is not unique on the source"));
	REQUIRE(twin.LastWrite().rows.size() == 2);
	REQUIRE(twin.store->transaction_ends.back() == "rollback");
	REQUIRE(Scalar(twin, "SELECT count(*) FROM dup WHERE v = 9") == Value::BIGINT(0));
}

TEST_CASE("a merge lands its inserts and updates on the source", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("MERGE INTO far.orders t USING (SELECT id, amt FROM far.archive) s ON t.id = s.id "
	                         "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'merged', s.amt, 0, NULL, NULL)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT name FROM orders WHERE id = 9") == Value("merged"));
}

TEST_CASE("a write returning rows returns what the source now holds", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL) RETURNING id, name");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(6));
	REQUIRE(result->GetValue(1, 0) == Value("zeta"));
}

TEST_CASE("a verb the source did not grant is refused at bind time and never reaches the source", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->verbs["orders"] = {CrossingVerb::SELECT, CrossingVerb::INSERT};
	twin.Seed();

	auto error = twin.Error("DELETE FROM far.orders WHERE id = 1");

	REQUIRE_THAT(error, Catch::Contains("'orders' does not have 'delete' permission"));
	REQUIRE(twin.store->writes.empty());
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a keyed write on a table with no key is refused at bind time", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto error = twin.Error("UPDATE far.sales SET amount = 0 WHERE region = 'east'");

	REQUIRE_THAT(error, Catch::Contains("'sales' has no key"));
	REQUIRE(twin.store->writes.empty());
}

TEST_CASE("a plan the source declines fails the statement with the source's reason", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.store->declined = "the source is read only today";

	auto error = twin.Error("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL)");

	REQUIRE_THAT(error, Catch::Contains("the source is read only today"));
	REQUIRE(twin.store->writes.empty());
}

TEST_CASE("a rolled back transaction leaves the source as it was", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
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
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
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
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Query("BEGIN");
	twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL)");
	auto during = twin.Query("SELECT count(*) FROM far.orders");
	twin.Query("COMMIT");

	REQUIRE(during->GetValue(0, 0) == Value::BIGINT(6));
	REQUIRE(twin.store->sessions_begun == 1);
}

TEST_CASE("each statement outside a transaction is its own session on the source", "[writes]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Query("SELECT count(*) FROM far.orders");
	twin.Query("SELECT count(*) FROM far.orders");

	REQUIRE(twin.store->transaction_ends == vector<string> {"commit", "commit"});
}
