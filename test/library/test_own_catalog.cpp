#include "catch.hpp"
#include "framework/twin.hpp"
#include "own_catalog/own_catalog.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"

using namespace duckdb;

namespace {

struct Own {
	shared_ptr<FarStore> store;
	DuckDB near;
	Connection con;

	Own() : store(make_shared_ptr<FarStore>(Transport::NATIVE)), near(nullptr), con(near) {
		ExtensionLoader loader(*near.instance, "owndb");
		own::OwnCatalog::Register(loader, store);
		Far("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
		Far("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
		Query("ATTACH 'own.example.com' AS own (TYPE owndb)");
	}

	void Far(const string &sql) {
		auto result = store->con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
	}

	unique_ptr<MaterializedQueryResult> Query(const string &sql) {
		auto result = con.Query(sql);
		store->JoinArrivals();
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		return result;
	}
};

} // namespace

TEST_CASE("a catalog of its own reads through the attach it holds", "[own_catalog]") {
	Own own;

	REQUIRE(own.Query("SELECT v FROM own.t WHERE id = 2")->GetValue(0, 0) == Value::INTEGER(20));
	REQUIRE(own.store->reads.size() == 1);
	REQUIRE(own.store->LastRead().Has(LogicalOperatorType::LOGICAL_FILTER));
}

TEST_CASE("a catalog of its own writes through the attach it holds and commits with it", "[own_catalog]") {
	Own own;

	own.Query("UPDATE own.t SET v = 0 WHERE id = 1");

	REQUIRE(own.store->writes.size() == 1);
	REQUIRE(own.store->transaction_ends == vector<string> {"commit"});
	REQUIRE(own.store->con.Query("SELECT v FROM t WHERE id = 1")->GetValue(0, 0) == Value::INTEGER(0));
}

TEST_CASE("a catalog of its own reaches its source type through the attach", "[own_catalog]") {
	Own own;
	own.con.BeginTransaction();
	auto &attached = *own.near.instance->GetDatabaseManager().GetDatabase(*own.con.context, "own");
	own.con.Commit();

	auto &attach = CrossingAttach::Of(attached.GetCatalog());
	REQUIRE(attach.Source<FarSource>().Tables("main") == vector<string> {"t"});
}
