#include "catch.hpp"
#include "internal/crossing_catalog.hpp"
#include "toy_source/toy_source.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

namespace {

struct Toy {
	shared_ptr<ToyStore> store = make_shared_ptr<ToyStore>();
	DuckDB db;
	Connection con;

	Toy() : db(nullptr), con(db) {
		ToyTable orders;
		orders.column_names = {"id", "name", "amt"};
		orders.column_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER};
		orders.key = {"id"};
		orders.rows = {{Value::INTEGER(1), Value("a"), Value::INTEGER(50)},
		               {Value::INTEGER(2), Value("b"), Value::INTEGER(150)},
		               {Value::INTEGER(3), Value("c"), Value::INTEGER(250)}};
		store->tables["orders"] = std::move(orders);

		ToyTable archive;
		archive.column_names = {"id", "name", "amt"};
		archive.column_types = {LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER};
		archive.rows = {{Value::INTEGER(9), Value("z"), Value::INTEGER(900)}};
		store->tables["archive"] = std::move(archive);

		ExtensionLoader loader(*db.instance, "toydb");
		ToySource::Register(loader, store);
	}

	~Toy() {
		store->JoinArrivals();
	}

	unique_ptr<MaterializedQueryResult> Query(const string &sql) {
		auto result = con.Query(sql);
		store->JoinArrivals();
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		return result;
	}

	string Error(const string &sql) {
		auto result = con.Query(sql);
		store->JoinArrivals();
		if (!result->HasError()) {
			FAIL("expected an error from: " + sql);
		}
		return result->GetError();
	}

	CrossingAttach &Attached(const string &alias = "toy") {
		con.BeginTransaction();
		auto &attached = *db.instance->GetDatabaseManager().GetDatabase(*con.context, alias);
		con.Commit();
		return attached.GetCatalog().Cast<CrossingCatalog>().Attach();
	}

	void Attach(const string &path = "toy.example.com", const string &alias = "toy") {
		Query("ATTACH '" + path + "' AS " + alias + " (TYPE toydb)");
	}
};

} // namespace

TEST_CASE("the attached path reaches the source and never becomes a file", "[toy]") {
	Toy toy;
	string path = "toydb_attach_path_is_not_a_file.db";

	toy.Attach(path);

	REQUIRE(toy.store->attached_paths == vector<string> {path});
	REQUIRE(!FileSystem::GetFileSystem(*toy.con.context).FileExists(path));
}

TEST_CASE("a read gets every row and the target does the filtering", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("SELECT name FROM toy.orders WHERE amt > 100 ORDER BY id");

	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0) == Value("b"));
	REQUIRE(result->GetValue(0, 1) == Value("c"));
}

TEST_CASE("a read names the floor's table", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("SELECT count(*) FROM toy.main.orders");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
}

TEST_CASE("an insert of literal rows fills the seam with them", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("INSERT INTO toy.orders VALUES (4, 'd', 400)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows.size() == 4);
	REQUIRE(toy.Query("SELECT amt FROM toy.orders WHERE id = 4")->GetValue(0, 0) == Value::INTEGER(400));
}

TEST_CASE("an insert from the same source fills the seam with its own read", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("INSERT INTO toy.orders SELECT * FROM toy.archive");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows.size() == 4);
	REQUIRE(toy.store->tables["orders"].rows[3][1] == Value("z"));
}

TEST_CASE("an update addresses rows by the key", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("UPDATE toy.orders SET amt = 0 WHERE id = 2");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows[1][2] == Value::INTEGER(0));
}

TEST_CASE("a delete addresses rows by the key", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("DELETE FROM toy.orders WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(toy.store->tables["orders"].rows.size() == 1);
}

TEST_CASE("a read shipped over the wire renders the same rows", "[toy]") {
	Toy toy;
	toy.store->ship_plans = true;
	toy.Attach();

	auto result = toy.Query("SELECT name FROM toy.orders WHERE amt > 100 ORDER BY id");

	REQUIRE(toy.store->plans_shipped == 1);
	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0) == Value("b"));
	REQUIRE(result->GetValue(0, 1) == Value("c"));
}

TEST_CASE("a write shipped over the wire carries its rows", "[toy]") {
	Toy toy;
	toy.store->ship_plans = true;
	toy.Attach();

	auto result = toy.Query("INSERT INTO toy.orders VALUES (4, 'd', 400)");

	REQUIRE(toy.store->plans_shipped == 1);
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows.size() == 4);
	REQUIRE(toy.store->tables["orders"].rows[3][2] == Value::INTEGER(400));
}

TEST_CASE("a write fed from the source ships and lands whole", "[toy]") {
	Toy toy;
	toy.store->ship_plans = true;
	toy.Attach();

	auto result = toy.Query("INSERT INTO toy.orders SELECT * FROM toy.archive");

	REQUIRE(toy.store->plans_shipped >= 1);
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows[3][1] == Value("z"));
}

TEST_CASE("two attaches on one connection are two sources", "[toy]") {
	Toy toy;
	toy.Attach("one", "a");
	toy.Attach("two", "b");

	auto result = toy.Query("SELECT (SELECT count(*) FROM a.orders) + (SELECT count(*) FROM b.orders)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(6));
	REQUIRE(toy.store->attached_paths == vector<string> {"one", "two"});
}

TEST_CASE("the catalog lists the source's tables and nothing else", "[toy]") {
	Toy toy;
	toy.Attach();

	auto result = toy.Query("SELECT table_name FROM duckdb_tables() WHERE database_name = 'toy' ORDER BY 1");

	REQUIRE(result->RowCount() == 2);
	REQUIRE(result->GetValue(0, 0) == Value("archive"));
	REQUIRE(result->GetValue(0, 1) == Value("orders"));
}

TEST_CASE("a table added after attach appears once the attach is refreshed", "[toy]") {
	Toy toy;
	toy.Attach();
	toy.Query("SELECT * FROM toy.orders");

	{
		lock_guard<mutex> guard(toy.store->lock);
		ToyTable late;
		late.column_names = {"id"};
		late.column_types = {LogicalType::INTEGER};
		late.rows = {{Value::INTEGER(7)}};
		toy.store->tables["late"] = std::move(late);
	}

	REQUIRE(toy.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'toy'")->GetValue(0, 0) ==
	        Value::BIGINT(2));
	toy.Error("SELECT * FROM toy.late");

	toy.Attached().Refresh();

	REQUIRE(toy.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'toy'")->GetValue(0, 0) ==
	        Value::BIGINT(3));
	REQUIRE(toy.Query("SELECT id FROM toy.late")->GetValue(0, 0) == Value::INTEGER(7));
}

TEST_CASE("refreshing one schema relists only that schema", "[toy]") {
	Toy toy;
	toy.Attach();
	toy.Query("SELECT * FROM toy.orders");
	toy.Query("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'toy'");
	auto aux_listings = toy.store->listings["aux"];
	auto main_listings = toy.store->listings["main"];

	{
		lock_guard<mutex> guard(toy.store->lock);
		ToyTable late;
		late.column_names = {"id"};
		late.column_types = {LogicalType::INTEGER};
		late.rows = {{Value::INTEGER(7)}};
		toy.store->tables["late"] = std::move(late);
	}
	toy.Error("SELECT * FROM toy.late");

	toy.Attached().Refresh("main");

	REQUIRE(toy.Query("SELECT id FROM toy.late")->GetValue(0, 0) == Value::INTEGER(7));
	REQUIRE(toy.store->listings["main"] == main_listings + 1);
	REQUIRE(toy.store->listings["aux"] == aux_listings);
}

TEST_CASE("schema names resolve regardless of case", "[toy]") {
	Toy toy;
	toy.Attach();

	REQUIRE(toy.Query("SELECT count(*) FROM toy.MAIN.orders")->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(toy.Query("SELECT count(*) FROM toy.ORDERS")->GetValue(0, 0) == Value::BIGINT(3));
}

TEST_CASE("nothing can be created in a served schema", "[toy]") {
	Toy toy;
	toy.Attach();

	REQUIRE_THAT(toy.Error("CREATE TABLE toy.main.scratch (x INTEGER)"),
	             Catch::Contains("served by a source; CREATE is not supported"));
	REQUIRE_THAT(toy.Error("CREATE SCHEMA toy.other"), Catch::Contains("schemas are served by the source"));
}

TEST_CASE("served tables cannot be dropped or altered", "[toy]") {
	Toy toy;
	toy.Attach();

	REQUIRE_THAT(toy.Error("DROP TABLE toy.orders"), Catch::Contains("'orders' is served by a source; DROP"));
	REQUIRE_THAT(toy.Error("ALTER TABLE toy.orders RENAME TO o"),
	             Catch::Contains("'orders' is served by a source; ALTER"));
	REQUIRE_THAT(toy.Error("DROP SCHEMA toy.main"), Catch::Contains("DETACH the catalog"));
	REQUIRE(toy.store->tables.size() == 2);
}

TEST_CASE("a transaction's end reaches the source", "[toy]") {
	Toy toy;
	toy.Attach();

	toy.Query("BEGIN");
	toy.Query("UPDATE toy.orders SET amt = 0 WHERE id = 2");
	toy.Query("ROLLBACK");
	toy.Query("BEGIN");
	toy.Query("SELECT count(*) FROM toy.orders");
	toy.Query("COMMIT");

	REQUIRE(toy.store->transaction_ends == vector<string> {"rollback", "commit"});
}

TEST_CASE("a partitioned reader is drained on every partition", "[toy]") {
	Toy toy;
	toy.store->read_partitions = 3;
	toy.Attach();

	auto result = toy.Query("SELECT id FROM toy.orders ORDER BY id");

	REQUIRE(result->RowCount() == 3);
	REQUIRE(result->GetValue(0, 0) == Value::INTEGER(1));
	REQUIRE(result->GetValue(0, 1) == Value::INTEGER(2));
	REQUIRE(result->GetValue(0, 2) == Value::INTEGER(3));
	REQUIRE(toy.store->partitions_read == set<idx_t> {0, 1, 2});
	REQUIRE(toy.Query("SELECT count(DISTINCT rowid) FROM toy.orders")->GetValue(0, 0) == Value::BIGINT(3));
}

TEST_CASE("a partitioned reader feeds a keyed write", "[toy]") {
	Toy toy;
	toy.store->read_partitions = 2;
	toy.Attach();

	auto result = toy.Query("UPDATE toy.orders SET amt = amt + 1 WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(toy.store->tables["orders"].rows[1][2] == Value::INTEGER(151));
	REQUIRE(toy.store->tables["orders"].rows[2][2] == Value::INTEGER(251));
}

TEST_CASE("a reader that waits is asked again once it is woken", "[toy]") {
	Toy toy;
	toy.store->wait_before_chunks = true;
	toy.Attach();

	auto result = toy.Query("SELECT id FROM toy.orders ORDER BY id");

	REQUIRE(result->RowCount() == 3);
	REQUIRE(result->GetValue(0, 2) == Value::INTEGER(3));
	REQUIRE(toy.store->waits_served == 2);
}

TEST_CASE("a wake that arrives before the pull parks is not lost", "[toy]") {
	Toy toy;
	toy.store->wait_before_chunks = true;
	toy.store->wake_before_returning = true;
	toy.Attach();

	auto result = toy.Query("SELECT id FROM toy.orders ORDER BY id");

	REQUIRE(result->RowCount() == 3);
	REQUIRE(toy.store->waits_served == 2);
}

TEST_CASE("a limit over parked partitioned readers returns and late wakes are harmless", "[toy]") {
	Toy toy;
	toy.store->wait_before_chunks = true;
	toy.store->keep_wakers = true;
	toy.store->read_partitions = 3;
	toy.Attach();

	auto result = toy.Query("SELECT id FROM toy.orders LIMIT 1");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(!toy.store->kept_wakers.empty());
	for (auto &waker : toy.store->kept_wakers) {
		waker.Wake();
	}
	REQUIRE(toy.Query("SELECT count(*) FROM toy.orders")->GetValue(0, 0) == Value::BIGINT(3));
}

TEST_CASE("a writer that waits is asked again once it is woken", "[toy]") {
	Toy toy;
	toy.store->wait_before_write = true;
	toy.Attach();

	auto result = toy.Query("INSERT INTO toy.orders VALUES (4, 'd', 400)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows.size() == 4);
	REQUIRE(toy.store->write_waits_served == 1);
}

TEST_CASE("a write run wholly on the source waits the same way", "[toy]") {
	Toy toy;
	toy.store->wait_before_write = true;
	toy.Attach();

	auto result = toy.Query("DELETE FROM toy.orders WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(toy.store->tables["orders"].rows.size() == 1);
	REQUIRE(toy.store->write_waits_served == 1);
}

TEST_CASE("a merge whose write waits finishes", "[toy]") {
	Toy toy;
	toy.store->wait_before_write = true;
	toy.Attach();

	auto result = toy.Query("MERGE INTO toy.orders t USING (SELECT id, amt FROM toy.archive) s ON t.id = s.id "
	                        "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'm', s.amt)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(toy.store->tables["orders"].rows.size() == 4);
	REQUIRE(toy.store->write_waits_served == 1);
}

TEST_CASE("a partitioned reader that waits is drained on every partition", "[toy]") {
	Toy toy;
	toy.store->wait_before_chunks = true;
	toy.store->read_partitions = 3;
	toy.Attach();

	auto result = toy.Query("SELECT count(*) FROM toy.orders");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(toy.store->partitions_read == set<idx_t> {0, 1, 2});
	REQUIRE(toy.store->waits_served == 6);
}

TEST_CASE("a detached source is gone", "[toy]") {
	Toy toy;
	toy.Attach();
	toy.Query("SELECT count(*) FROM toy.orders");

	toy.Query("DETACH toy");

	REQUIRE(toy.store->detached_paths == vector<string> {"toy.example.com"});
	REQUIRE_THAT(toy.Error("SELECT count(*) FROM toy.orders"), Catch::Contains("toy"));
	toy.Attach("again", "toy");
	REQUIRE(toy.Query("SELECT count(*) FROM toy.orders")->GetValue(0, 0) == Value::BIGINT(3));
}
