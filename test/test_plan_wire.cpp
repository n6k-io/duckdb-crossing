#include "framework/stub_seam.hpp"
#include "internal/plan_wire.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

using namespace duckdb;

namespace {

struct FarSide {
	DuckDB db;
	Connection con;

	FarSide() : db(nullptr), con(db) {
		RegisterCrossingPlanFunctions(*db.instance);
		con.BeginTransaction();
	}
	~FarSide() {
		con.Rollback();
	}

	unique_ptr<LogicalOperator> Receive(const LogicalOperator &plan) {
		return DeserializeCrossingPlan(*con.context, SerializeCrossingPlan(plan));
	}
};

unique_ptr<LogicalOperator> FloorUnderFilterAndProjection() {
	auto floor = MakeFloorNode(5, "main", "orders", {"id", "amt"}, {LogicalType::INTEGER, LogicalType::INTEGER});
	auto bindings = floor->GetColumnBindings();

	auto amt = make_uniq<BoundColumnRefExpression>("amt", LogicalType::INTEGER, bindings[1]);
	auto predicate = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(amt),
	                                                      make_uniq<BoundConstantExpression>(Value::INTEGER(100)));
	auto filter = make_uniq<LogicalFilter>(std::move(predicate));
	filter->children.push_back(std::move(floor));

	vector<unique_ptr<Expression>> select_list;
	select_list.push_back(make_uniq<BoundColumnRefExpression>("id", LogicalType::INTEGER, bindings[0]));
	auto projection = make_uniq<LogicalProjection>(6, std::move(select_list));
	projection->children.push_back(std::move(filter));
	projection->ResolveOperatorTypes();
	return std::move(projection);
}

} // namespace

TEST_CASE("a read plan survives the wire with its floor intact", "[plan_wire]") {
	FarSide far;
	auto sent = FloorUnderFilterAndProjection();

	auto received = far.Receive(*sent);

	REQUIRE(received->type == LogicalOperatorType::LOGICAL_PROJECTION);
	REQUIRE(received->children[0]->type == LogicalOperatorType::LOGICAL_FILTER);
	auto &floor_node = *received->children[0]->children[0];
	auto floor = FloorOf(floor_node);
	REQUIRE(floor);
	REQUIRE(floor->schema == "main");
	REQUIRE(floor->table == "orders");
	REQUIRE(floor->column_names == vector<string> {"id", "amt"});
	REQUIRE(floor_node.Cast<LogicalGet>().table_index == 5);
	REQUIRE(received->ToString() == sent->ToString());
}

TEST_CASE("the tables a received plan names are the tables the sent plan named", "[plan_wire]") {
	FarSide far;
	auto sent = FloorUnderFilterAndProjection();

	auto received = far.Receive(*sent);

	auto before = CrossingTablesOf(*sent);
	auto after = CrossingTablesOf(*received);
	REQUIRE(after.size() == 1);
	REQUIRE(after[0].schema == before[0].schema);
	REQUIRE(after[0].table == before[0].table);
	REQUIRE(after[0].columns == before[0].columns);
}

TEST_CASE("a seam survives the wire", "[plan_wire]") {
	FarSide far;
	auto fragment = FragmentOverSeam({LogicalType::INTEGER, LogicalType::VARCHAR});

	auto received = far.Receive(*fragment->plan);

	CrossingFragment landed;
	landed.plan = std::move(received);
	auto slot = landed.SeamSlot();
	REQUIRE(slot);
	auto &seam = (*slot)->Cast<LogicalGet>();
	REQUIRE(seam.table_index == STUB_SEAM_INDEX);
	REQUIRE(seam.returned_types == vector<LogicalType> {LogicalType::INTEGER, LogicalType::VARCHAR});
}

TEST_CASE("a filled seam carries its rows across the wire", "[plan_wire]") {
	FarSide far;
	auto fragment = FragmentOverSeam({LogicalType::INTEGER, LogicalType::INTEGER});
	*fragment->SeamSlot() = Rows();

	auto received = far.Receive(*fragment->plan);

	auto rows = SeamRowsOf(*received->children[0]);
	REQUIRE(rows);
	REQUIRE(rows->Count() == 1);
}
