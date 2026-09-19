#include "catch.hpp"
#include "read_only_source/read_only_source.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

TEST_CASE("a source whose session has no Write reads and refuses writes", "[read_only]") {
	DuckDB db(nullptr);
	Connection con(db);
	ExtensionLoader loader(*db.instance, "rodb");
	readonly::ReadOnlySource::Register(loader);

	auto attached = con.Query("ATTACH 'nowhere' AS ro (TYPE rodb)");
	REQUIRE(!attached->HasError());

	auto count = con.Query("SELECT count(*) FROM ro.t");
	REQUIRE(!count->HasError());
	REQUIRE(count->GetValue(0, 0) == Value::BIGINT(0));

	auto insert = con.Query("INSERT INTO ro.t VALUES (1)");
	REQUIRE(insert->HasError());
	REQUIRE_THAT(insert->GetError(), Catch::Contains("does not have 'insert' permission"));
}

TEST_CASE("a table declaring a write verb on a source whose session has no Write is refused on first use",
          "[read_only]") {
	DuckDB db(nullptr);
	Connection con(db);
	ExtensionLoader loader(*db.instance, "rodb");
	readonly::ReadOnlySource::Register(loader);

	auto attached = con.Query("ATTACH 'nowhere' AS ro (TYPE rodb)");
	REQUIRE(!attached->HasError());

	auto count = con.Query("SELECT count(*) FROM ro.w");
	REQUIRE(count->HasError());
	REQUIRE_THAT(count->GetError(), Catch::Contains("'w' declares 'insert' but the session has no Write"));
}
