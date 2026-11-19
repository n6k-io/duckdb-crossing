#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

TEST_CASE("an aggregate ORDER BY runs on the far side", "[pushdown][aggregate_order]") {
	Twin twin;
	twin.Seed();

	twin.Same("SELECT list(id ORDER BY id DESC) FROM far.orders");
	REQUIRE(twin.DidCross(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
}

TEST_CASE("a window aggregate ORDER BY runs on the far side", "[pushdown][aggregate_order]") {
	Twin twin;
	twin.Seed();

	twin.Same("SELECT list(id ORDER BY id DESC) OVER () FROM far.orders");
	REQUIRE(twin.DidCross(Op::LOGICAL_WINDOW));
}
