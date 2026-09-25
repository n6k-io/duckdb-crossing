#include "framework/twin.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/pending_query_result.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

namespace {

constexpr const char *STORE_CATALOG = "memory";

struct Described {
	vector<string> names;
	vector<LogicalType> types;
};

Described DescribeOn(Connection &con, const string &table) {
	auto info = con.TableInfo(STORE_CATALOG, "main", table);
	REQUIRE(info);
	Described described;
	for (auto &column : info->columns) {
		described.names.push_back(column.Name());
		described.types.push_back(column.Type());
	}
	return described;
}

unique_ptr<LogicalOperator> FloorOver(Connection &con, idx_t index, const string &table) {
	auto described = DescribeOn(con, table);
	return MakeFloorNode(index, "main", table, described.names, described.types);
}

idx_t RowsFrom(Connection &con, unique_ptr<LogicalOperator> plan) {
	auto pending = PendingCrossingPlan(*con.context, std::move(plan), false);
	if (pending->HasError()) {
		pending->ThrowError();
	}
	auto result = pending->Execute();
	if (result->HasError()) {
		result->ThrowError();
	}
	idx_t rows = 0;
	while (auto chunk = result->Fetch()) {
		rows += chunk->size();
	}
	return rows;
}

Value Scalar(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return result->GetValue(0, 0);
}

unique_ptr<ColumnDataCollection> Rows(const vector<LogicalType> &types, const vector<vector<Value>> &values) {
	auto rows = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t r = 0; r < values.size(); r++) {
		for (idx_t c = 0; c < types.size(); c++) {
			chunk.SetValue(c, r, values[r][c]);
		}
	}
	chunk.SetCardinality(values.size());
	rows->Append(chunk);
	return rows;
}

CrossingWriteTarget TargetOf(CrossingVerb verb, const string &table, vector<string> key_columns,
                             vector<string> set_columns) {
	CrossingWriteTarget target;
	target.catalog = STORE_CATALOG;
	target.schema = "main";
	target.table = table;
	target.verb = verb;
	target.key_columns = std::move(key_columns);
	target.set_columns = std::move(set_columns);
	return target;
}

struct InTransaction {
	explicit InTransaction(Connection &con_p) : con(con_p) {
		con.BeginTransaction();
	}
	~InTransaction() {
		con.Rollback();
	}
	Connection &con;
};

//! A source that hands crossing its own bound scan instead of a floor.
class RawScanSource : public FarSource {
public:
	RawScanSource(shared_ptr<FarStore> store_p, string path) : FarSource(store_p, std::move(path)), raw(store_p) {
	}

	CrossingPlan Plan(const CrossingPlanRequest &request) {
		if (request.verb != CrossingVerb::SELECT) {
			return FarSource::Plan(request);
		}
		lock_guard<mutex> guard(raw->lock);
		InTransaction txn(raw->con);
		Planner planner(*raw->con.context);
		planner.CreatePlan(make_uniq<RelationStatement>(raw->con.Table(STORE_CATALOG, request.schema, request.table)));
		return CrossingPlan::Of(std::move(planner.plan));
	}

	static void Register(ExtensionLoader &loader, shared_ptr<FarStore> store) {
		Crossing<RawScanSource>::Register(loader, "rawdb", [store](ClientContext &, AttachInfo &info) {
			return make_uniq<RawScanSource>(store, info.path);
		});
	}

private:
	shared_ptr<FarStore> raw;
};

} // namespace

TEST_CASE("BindFloors binds a floor to its table under the floor's index", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	InTransaction txn(con);

	auto plan = FloorOver(con, 7, "orders");
	BindFloors(*con.context, plan, STORE_CATALOG);

	REQUIRE(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = plan->Cast<LogicalGet>();
	REQUIRE(!FloorOf(get));
	REQUIRE(get.table_index == 7);
	REQUIRE(get.GetColumnBindings().size() == 6);
	REQUIRE(RowsFrom(con, std::move(plan)) == 5);
}

TEST_CASE("BindFloors refuses a table that no longer matches the floor", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	InTransaction txn(con);

	auto plan = MakeFloorNode(0, "main", "orders", {"id"}, {LogicalType::INTEGER});

	REQUIRE_THROWS_WITH(BindFloors(*con.context, plan, STORE_CATALOG), Catch::Contains("changed on the source"));
}

TEST_CASE("BindFloors lets a resolver stand a filtered subquery in for a floor", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	InTransaction txn(con);

	auto plan = FloorOver(con, 3, "orders");
	CrossingFloorResolver resolver = [&con](const CrossingFloor &floor) {
		return con.Table(STORE_CATALOG, floor.schema, floor.table)->Filter("amt > 100")->GetTableRef();
	};
	BindFloors(*con.context, plan, STORE_CATALOG, resolver);

	REQUIRE(plan->type != LogicalOperatorType::LOGICAL_GET);
	for (auto &binding : plan->GetColumnBindings()) {
		REQUIRE(binding.table_index == 3);
	}
	REQUIRE(RowsFrom(con, std::move(plan)) == 4);
}

TEST_CASE("BindFloors needs a transaction", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;

	auto plan = FloorOver(con, 0, "orders");

	REQUIRE_THROWS_WITH(BindFloors(*con.context, plan, STORE_CATALOG), Catch::Contains("transaction"));
}

TEST_CASE("ExecuteWrite deletes the rows the seam names", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	con.BeginTransaction();

	auto rows = Rows({LogicalType::INTEGER}, {{Value::INTEGER(1)}, {Value::INTEGER(3)}});
	auto changed =
	    ExecuteWrite(*con.context, TargetOf(CrossingVerb::DELETE_, "orders", {"id"}, {}), SeamRefOfRows(*rows));
	con.Commit();

	REQUIRE(changed == 2);
	REQUIRE(Scalar(con, "SELECT count(*) FROM orders") == Value::BIGINT(3));
}

TEST_CASE("ExecuteWrite matches a NULL key", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Far("CREATE TABLE nk(k INTEGER, v INTEGER)");
	twin.Far("INSERT INTO nk VALUES (NULL, 1), (2, 1)");
	auto &con = twin.store->con;
	con.BeginTransaction();

	auto rows = Rows({LogicalType::INTEGER, LogicalType::INTEGER}, {{Value(LogicalType::INTEGER), Value::INTEGER(5)}});
	auto changed = ExecuteWrite(*con.context, TargetOf(CrossingVerb::UPDATE, "nk", {"k"}, {"v"}), SeamRefOfRows(*rows));
	con.Commit();

	REQUIRE(changed == 1);
	REQUIRE(Scalar(con, "SELECT v FROM nk WHERE k IS NULL") == Value::INTEGER(5));
	REQUIRE(Scalar(con, "SELECT v FROM nk WHERE k = 2") == Value::INTEGER(1));
}

TEST_CASE("ExecuteWrite refuses a seam narrower than the write", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	InTransaction txn(con);

	auto rows = Rows({LogicalType::INTEGER}, {{Value::INTEGER(1)}});

	REQUIRE_THROWS(
	    ExecuteWrite(*con.context, TargetOf(CrossingVerb::UPDATE, "orders", {"id"}, {"amt"}), SeamRefOfRows(*rows)));
}

TEST_CASE("ExecuteWrite refuses a keyed write with no key", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	InTransaction txn(con);

	auto rows = Rows({LogicalType::INTEGER}, {{Value::INTEGER(1)}});

	REQUIRE_THROWS_WITH(
	    ExecuteWrite(*con.context, TargetOf(CrossingVerb::DELETE_, "orders", {}, {}), SeamRefOfRows(*rows)),
	    Catch::Contains("no key columns"));
}

TEST_CASE("ExecuteWrite feeds a write from a bound plan whose indices collide with the statement's", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	con.BeginTransaction();

	auto feed = FloorOver(con, 0, "archive");
	BindFloors(*con.context, feed, STORE_CATALOG);
	auto columns = DescribeOn(con, "orders").names;
	auto changed = ExecuteWrite(*con.context, TargetOf(CrossingVerb::INSERT, "orders", {}, columns), std::move(feed));
	con.Commit();

	REQUIRE(changed == 1);
	REQUIRE(Scalar(con, "SELECT name FROM orders WHERE id = 9") == Value("zeta"));
}

TEST_CASE("ExecuteWrite lets a shaper narrow the statement before it binds", "[execute]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	auto &con = twin.store->con;
	con.BeginTransaction();

	auto rows = Rows({LogicalType::INTEGER, LogicalType::INTEGER},
	                 {{Value::INTEGER(1), Value::INTEGER(0)}, {Value::INTEGER(2), Value::INTEGER(0)}});
	CrossingWriteShaper only_large = [](CrossingWriteStatement &built) {
		auto &update = built.statement->Cast<UpdateStatement>();
		auto large = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN,
		                                             make_uniq<ColumnRefExpression>("amt", "orders"),
		                                             make_uniq<ConstantExpression>(Value::INTEGER(100)));
		update.set_info->condition = make_uniq<ConjunctionExpression>(
		    ExpressionType::CONJUNCTION_AND, std::move(update.set_info->condition), std::move(large));
	};
	auto changed = ExecuteWrite(*con.context, TargetOf(CrossingVerb::UPDATE, "orders", {"id"}, {"amt"}),
	                            SeamRefOfRows(*rows), only_large);
	con.Commit();

	REQUIRE(changed == 1);
	REQUIRE(Scalar(con, "SELECT amt FROM orders WHERE id = 1") == Value::INTEGER(50));
	REQUIRE(Scalar(con, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(0));
}

TEST_CASE("a NULL key row reaches the source through a crossing and is updated", "[execute]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->keys["nk"] = {"k"};
	twin.store->key_unique["nk"] = false;
	twin.Far("CREATE TABLE nk(k INTEGER, v INTEGER)");
	twin.Far("INSERT INTO nk VALUES (NULL, 1), (2, 1), (3, 9)");
	twin.Attach();

	auto result = twin.Query("UPDATE far.nk SET v = 5 WHERE v = 1");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(Scalar(twin.store->con, "SELECT count(*) FROM nk WHERE v = 5") == Value::BIGINT(2));
}

TEST_CASE("a source whose plan holds its own scan is refused at bind", "[execute]") {
	auto store = make_shared_ptr<FarStore>(Transport::NATIVE);
	DuckDB near(nullptr);
	Connection con(near);
	ExtensionLoader loader(*near.instance, "rawdb");
	RawScanSource::Register(loader, store);
	REQUIRE(!store->con.Query("CREATE TABLE orders(id INTEGER)")->HasError());
	REQUIRE(!con.Query("ATTACH 'raw.example.com' AS raw (TYPE rawdb)")->HasError());

	auto result = con.Query("SELECT * FROM raw.orders");

	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("MakeFloorNode") != string::npos);
	REQUIRE(result->GetError().find("seq_scan") != string::npos);
}
