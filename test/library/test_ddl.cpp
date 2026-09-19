#include "framework/twin.hpp"

using namespace duckdb;

namespace {

Value FarScalar(Twin &twin, const string &sql) {
	auto result = twin.store->con.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return result->GetValue(0, 0);
}

Value FarTableCount(Twin &twin, const string &name) {
	return FarScalar(twin, "SELECT count(*) FROM duckdb_tables() WHERE table_name = '" + name + "'");
}

} // namespace

TEST_CASE("create table reaches the source and the table is served at once", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Query("CREATE TABLE far.main.scratch(id INTEGER PRIMARY KEY, v VARCHAR)");

	REQUIRE(FarTableCount(twin, "scratch") == Value::BIGINT(1));
	REQUIRE(twin.store->ddls.size() == 1);
	REQUIRE(twin.store->ddls[0].verb == CrossingVerb::CREATE);
	REQUIRE(twin.store->ddls[0].table == "scratch");
	twin.Query("INSERT INTO far.main.scratch VALUES (1, 'a')");
	twin.Same("SELECT id, v FROM far.scratch ORDER BY id");
}

TEST_CASE("alter table reaches the source and the table is re-described", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Query("SELECT * FROM far.orders");

	twin.Query("ALTER TABLE far.orders ADD COLUMN extra INTEGER DEFAULT 1");

	REQUIRE(twin.store->ddls.size() == 1);
	REQUIRE(twin.store->ddls[0].verb == CrossingVerb::ALTER);
	twin.Same("SELECT id, extra FROM far.orders ORDER BY id");
}

TEST_CASE("rename moves the served name", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Query("ALTER TABLE far.orders RENAME TO o");

	REQUIRE(FarTableCount(twin, "o") == Value::BIGINT(1));
	REQUIRE(FarTableCount(twin, "orders") == Value::BIGINT(0));
	twin.Same("SELECT count(*) FROM far.o");
	REQUIRE_THAT(twin.Error("SELECT * FROM far.orders"), Catch::Contains("orders"));
}

TEST_CASE("drop table reaches the source and the table is gone", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Query("SELECT * FROM far.orders");

	twin.Query("DROP TABLE far.orders");

	REQUIRE(FarTableCount(twin, "orders") == Value::BIGINT(0));
	REQUIRE(twin.store->ddls.size() == 1);
	REQUIRE(twin.store->ddls[0].verb == CrossingVerb::DROP);
	REQUIRE_THAT(twin.Error("SELECT * FROM far.orders"), Catch::Contains("orders"));
	twin.Query("DROP TABLE IF EXISTS far.orders");
	REQUIRE(twin.store->ddls.size() == 1);
}

TEST_CASE("create table as lands the rows on the source", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	auto count = twin.Query("CREATE TABLE far.main.copy AS SELECT id, amt FROM far.orders WHERE amt > 100");

	REQUIRE(count->GetValue(0, 0) == Value::BIGINT(4));
	REQUIRE(twin.store->ddls.size() == 1);
	REQUIRE(twin.store->ddls[0].verb == CrossingVerb::CREATE);
	REQUIRE(twin.LastWrite().kind == CrossingVerb::INSERT);
	REQUIRE(twin.LastWrite().written.table == "copy");
	twin.Same("SELECT id, amt FROM far.copy ORDER BY id");
	REQUIRE(FarScalar(twin, "SELECT count(*) FROM copy") == Value::BIGINT(4));
}

TEST_CASE("create table as from the target's own rows", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Query("CREATE TABLE local(x INTEGER)");
	twin.Query("INSERT INTO local VALUES (1), (2)");

	twin.Query("CREATE TABLE far.main.fromlocal AS SELECT x FROM local");

	REQUIRE(FarScalar(twin, "SELECT sum(x) FROM fromlocal") == Value::HUGEINT(3));
}

TEST_CASE("create table as of nothing still creates the table", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Query("CREATE TABLE far.main.empty AS SELECT id FROM far.orders WHERE false");

	REQUIRE(FarTableCount(twin, "empty") == Value::BIGINT(1));
	REQUIRE(FarScalar(twin, "SELECT count(*) FROM empty") == Value::BIGINT(0));
}

TEST_CASE("create table as is refused without create, and without insert on the created table", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.store->schema_verbs["main"] = {};
	twin.store->verbs["ro"] = {CrossingVerb::SELECT};
	twin.Seed();

	REQUIRE_THAT(twin.Error("CREATE TABLE far.main.c AS SELECT 1 AS x"),
	             Catch::Contains("schema 'main' does not have 'create' permission"));
	REQUIRE(twin.store->ddls.empty());

	twin.store->schema_verbs["main"] = {CrossingVerb::CREATE};
	twin.Query("DETACH far");
	twin.Attach();
	REQUIRE_THAT(twin.Error("CREATE TABLE far.main.ro AS SELECT 1 AS x"),
	             Catch::Contains("'ro' does not have 'insert' permission"));
	REQUIRE(FarTableCount(twin, "ro") == Value::BIGINT(0));
}

TEST_CASE("ddl rolls back and commits with the transaction", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.gone(x INTEGER)");
	twin.Query("ROLLBACK");
	REQUIRE(FarTableCount(twin, "gone") == Value::BIGINT(0));

	twin.Query("BEGIN");
	twin.Query("CREATE TABLE far.main.kept(x INTEGER)");
	twin.Query("COMMIT");
	REQUIRE(FarTableCount(twin, "kept") == Value::BIGINT(1));
}

TEST_CASE("create or replace needs drop on the served table", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.store->verbs["orders"] = {CrossingVerb::SELECT};
	twin.Seed();

	for (auto sql : {"CREATE OR REPLACE TABLE far.main.orders(x INTEGER)",
	                 "CREATE OR REPLACE TABLE far.main.orders AS SELECT 1 AS x"}) {
		REQUIRE_THAT(twin.Error(sql), Catch::Contains("'orders' does not have 'drop' permission"));
	}
	REQUIRE(twin.store->ddls.empty());
	REQUIRE(FarScalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a table dropped in this transaction is missing, not unsupported", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	SECTION("drop if exists is a no-op") {
		twin.Query("BEGIN");
		twin.Query("DROP TABLE far.orders");
		twin.Query("DROP TABLE IF EXISTS far.orders");
		REQUIRE(twin.store->ddls.size() == 1);
		twin.Query("COMMIT");
	}

	SECTION("plain drop reports the missing entry") {
		twin.Query("BEGIN");
		twin.Query("DROP TABLE far.orders");
		auto error = twin.Error("DROP TABLE far.orders");
		REQUIRE_THAT(error, Catch::Contains("orders"));
		REQUIRE_THAT(error, !Catch::Contains("not supported"));
		twin.Query("ROLLBACK");
	}

	SECTION("alter if exists is a no-op") {
		twin.Query("BEGIN");
		twin.Query("DROP TABLE far.orders");
		twin.Query("ALTER TABLE IF EXISTS far.orders ADD COLUMN extra INTEGER");
		REQUIRE(twin.store->ddls.size() == 1);
		twin.Query("ROLLBACK");
	}

	SECTION("plain alter reports the missing entry") {
		twin.Query("BEGIN");
		twin.Query("DROP TABLE far.orders");
		auto error = twin.Error("ALTER TABLE far.orders ADD COLUMN extra INTEGER");
		REQUIRE_THAT(error, Catch::Contains("orders"));
		REQUIRE_THAT(error, !Catch::Contains("not supported"));
		twin.Query("ROLLBACK");
	}

	SECTION("the old name after a rename") {
		twin.Query("BEGIN");
		twin.Query("ALTER TABLE far.orders RENAME TO o");
		twin.Query("DROP TABLE IF EXISTS far.orders");
		REQUIRE(twin.store->ddls.size() == 1);
		auto error = twin.Error("DROP TABLE far.orders");
		REQUIRE_THAT(error, Catch::Contains("orders"));
		REQUIRE_THAT(error, !Catch::Contains("not supported"));
		twin.Query("ROLLBACK");
	}
}

TEST_CASE("concurrent ddl transactions do not see each other's pending schema", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	Connection other(twin.near);
	auto other_query = [&](const string &sql) {
		auto result = other.Query(sql);
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		return result;
	};

	twin.Query("BEGIN");
	other_query("BEGIN");
	twin.Query("CREATE TABLE far.main.mine(x INTEGER)");
	other_query("CREATE TABLE far.main.theirs(y INTEGER)");

	twin.Query("DROP TABLE far.orders");

	REQUIRE(other_query("SELECT count(*) FROM far.orders")->GetValue(0, 0) == Value::BIGINT(5));
	REQUIRE(other_query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far' AND table_name IN "
	                    "('orders', 'theirs')")
	            ->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(other_query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far' AND table_name = 'mine'")
	            ->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE(twin.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far' AND table_name = 'theirs'")
	            ->GetValue(0, 0) == Value::BIGINT(0));

	twin.Query("ROLLBACK");
	other_query("ROLLBACK");
}

TEST_CASE("uncommitted ddl is invisible to other transactions", "[ddl]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	Connection other(twin.near);
	auto other_scalar = [&](const string &sql) {
		auto result = other.Query(sql);
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		return result->GetValue(0, 0);
	};
	auto other_fails = [&](const string &sql) {
		return other.Query(sql)->HasError();
	};

	SECTION("create") {
		twin.Query("BEGIN");
		twin.Query("CREATE TABLE far.main.pending(x INTEGER)");
		REQUIRE(twin.Query("SELECT count(*) FROM far.pending")->GetValue(0, 0) == Value::BIGINT(0));

		REQUIRE(other_scalar("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far' AND table_name = "
		                     "'pending'") == Value::BIGINT(0));
		REQUIRE(other_fails("SELECT * FROM far.pending"));

		twin.Query("COMMIT");
		REQUIRE(other_scalar("SELECT count(*) FROM far.pending") == Value::BIGINT(0));
	}

	SECTION("drop") {
		twin.Query("BEGIN");
		twin.Query("DROP TABLE far.orders");
		REQUIRE(twin.Error("SELECT * FROM far.orders").find("orders") != string::npos);

		REQUIRE(other_scalar("SELECT count(*) FROM far.orders") == Value::BIGINT(5));

		twin.Query("ROLLBACK");
		REQUIRE(other_scalar("SELECT count(*) FROM far.orders") == Value::BIGINT(5));
		twin.Same("SELECT count(*) FROM far.orders");
	}

	SECTION("alter") {
		twin.Query("BEGIN");
		twin.Query("ALTER TABLE far.orders ADD COLUMN extra INTEGER DEFAULT 1");
		REQUIRE(twin.Query("SELECT extra FROM far.orders LIMIT 1")->GetValue(0, 0) == Value::INTEGER(1));

		REQUIRE(other_fails("SELECT extra FROM far.orders"));

		twin.Query("COMMIT");
		REQUIRE(other_scalar("SELECT extra FROM far.orders LIMIT 1") == Value::INTEGER(1));
	}
}
