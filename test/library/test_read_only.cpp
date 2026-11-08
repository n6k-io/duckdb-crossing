#include "framework/twin.hpp"

using namespace duckdb;

TEST_CASE("a table granting only select reads and refuses writes", "[read_only]") {
	Twin twin;
	twin.Far("CREATE TABLE t(id INTEGER)");
	twin.store->verbs["t"] = {CrossingVerb::SELECT};
	twin.Attach();

	REQUIRE(twin.Query("SELECT count(*) FROM far.t")->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE_THAT(twin.Error("INSERT INTO far.t VALUES (1)"), Catch::Contains("does not have 'insert' permission"));
}

TEST_CASE("a schema that does not declare create refuses it", "[read_only]") {
	Twin twin;
	twin.store->schema_verbs["main"] = {};
	twin.Attach();

	REQUIRE_THAT(twin.Error("CREATE TABLE far.main.n(id INTEGER)"),
	             Catch::Contains("schema 'main' does not have 'create' permission"));
}
