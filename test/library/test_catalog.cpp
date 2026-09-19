#include "crossing_attach.hpp"
#include "framework/twin.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

using namespace duckdb;

namespace {

CrossingAttach &AttachedAs(Twin &twin, const string &alias) {
	twin.con.BeginTransaction();
	auto &attached = *twin.near.instance->GetDatabaseManager().GetDatabase(*twin.con.context, alias);
	twin.con.Commit();
	return CrossingAttach::Of(attached.GetCatalog());
}

} // namespace

TEST_CASE("the attached path reaches the source and never becomes a file", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	string path = "fardb_attach_path_is_not_a_file.db";

	twin.Attach(path, "p");

	REQUIRE(twin.store->attached_paths == vector<string> {"far.example.com", path});
	REQUIRE(!FileSystem::GetFileSystem(*twin.con.context).FileExists(path));
}

TEST_CASE("the catalog lists the source's schemas and tables and nothing else", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Far("CREATE SCHEMA aux");
	twin.Far("CREATE TABLE aux.side(x INTEGER)");
	twin.Seed();

	auto tables = twin.Query("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'far' ORDER "
	                         "BY 1, 2");

	REQUIRE(tables->RowCount() == 6);
	REQUIRE(tables->GetValue(0, 0) == Value("aux"));
	REQUIRE(tables->GetValue(1, 0) == Value("side"));
	REQUIRE(tables->GetValue(1, 1) == Value("archive"));
	REQUIRE(tables->GetValue(1, 5) == Value("types_t"));
	twin.Same("SELECT x FROM far.aux.side");
}

TEST_CASE("a table added after attach appears once the attach is refreshed", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Query("SELECT * FROM far.orders");

	twin.Far("CREATE TABLE late(id INTEGER)");
	twin.Far("INSERT INTO late VALUES (7)");

	REQUIRE(twin.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far'")->GetValue(0, 0) ==
	        Value::BIGINT(5));
	REQUIRE_THAT(twin.Error("SELECT * FROM far.late"), Catch::Contains("late"));

	AttachedAs(twin, "far").Refresh();

	REQUIRE(twin.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far'")->GetValue(0, 0) ==
	        Value::BIGINT(6));
	twin.Same("SELECT id FROM far.late");
}

TEST_CASE("refreshing one schema relists only that schema", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Far("CREATE SCHEMA aux");
	twin.Seed();
	twin.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far'");
	auto aux_listings = twin.store->listings["aux"];
	auto main_listings = twin.store->listings["main"];
	twin.Far("CREATE TABLE late(id INTEGER)");

	AttachedAs(twin, "far").Refresh("main");

	twin.Same("SELECT id FROM far.late");
	REQUIRE(twin.store->listings["main"] == main_listings + 1);
	REQUIRE(twin.store->listings["aux"] == aux_listings);
}

TEST_CASE("a description is taken once per table for the life of the attach", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Query("SELECT id FROM far.orders");
	twin.Far("ALTER TABLE orders ADD COLUMN extra INTEGER DEFAULT 1");
	auto error = twin.Error("SELECT * FROM far.orders LIMIT 1");
	REQUIRE_THAT(error, Catch::Contains("Catalog Error"));
	REQUIRE_THAT(error, Catch::Contains("'orders' changed on the source"));
	REQUIRE_THAT(error, !Catch::Contains("INTERNAL"));

	AttachedAs(twin, "far").Refresh();

	auto after = twin.Query("SELECT * FROM far.orders LIMIT 1");
	REQUIRE(after->ColumnCount() == 7);
}

TEST_CASE("schema and table names resolve regardless of case", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Same("SELECT count(*) FROM far.MAIN.orders", "SELECT count(*) FROM main.orders");
	twin.Same("SELECT count(*) FROM far.ORDERS", "SELECT count(*) FROM orders");
	twin.Same("SELECT ID, Amt FROM far.orders ORDER BY id");
}

TEST_CASE("nothing can be created in a served schema", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	REQUIRE_THAT(twin.Error("CREATE TABLE far.main.scratch (x INTEGER)"),
	             Catch::Contains("served by a source; CREATE is not supported"));
	REQUIRE_THAT(twin.Error("CREATE SCHEMA far.other"), Catch::Contains("schemas are served by the source"));
	REQUIRE_THAT(twin.Error("CREATE VIEW far.main.v AS SELECT 1"), Catch::Contains("served by a source"));
	REQUIRE(twin.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'far'")->GetValue(0, 0) ==
	        Value::BIGINT(5));
}

TEST_CASE("served tables cannot be dropped or altered", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	REQUIRE_THAT(twin.Error("DROP TABLE far.orders"), Catch::Contains("'orders' is served by a source; DROP"));
	REQUIRE_THAT(twin.Error("ALTER TABLE far.orders RENAME TO o"),
	             Catch::Contains("'orders' is served by a source; ALTER"));
	REQUIRE_THAT(twin.Error("DROP SCHEMA far.main"), Catch::Contains("DETACH the catalog"));
	twin.Same("SELECT count(*) FROM far.orders");
}

TEST_CASE("a detached source is told, and is gone", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Query("SELECT count(*) FROM far.orders");

	twin.Query("DETACH far");

	REQUIRE(twin.store->detached_paths == vector<string> {"far.example.com"});
	REQUIRE_THAT(twin.Error("SELECT count(*) FROM far.orders"), Catch::Contains("far"));
	twin.Attach("again", "far");
	twin.Same("SELECT count(*) FROM far.orders");
}

TEST_CASE("two attaches on one connection are two sources", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Attach("two", "b");

	twin.Same("SELECT (SELECT count(*) FROM far.orders) + (SELECT count(*) FROM b.orders)",
	          "SELECT (SELECT count(*) FROM orders) + (SELECT count(*) FROM orders)");

	REQUIRE(twin.store->attached_paths == vector<string> {"far.example.com", "two"});
	REQUIRE(twin.store->reads.size() == 2);
}

TEST_CASE("the attach lists the source lazily, not at ATTACH", "[catalog]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	REQUIRE(twin.store->listings.empty());

	twin.Query("SELECT count(*) FROM far.orders");

	REQUIRE(twin.store->listings["main"] >= 1);
}
