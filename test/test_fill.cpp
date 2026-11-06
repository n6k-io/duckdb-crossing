#include "internal/fill.hpp"
#include "framework/plan_builder.hpp"

using namespace duckdb;

namespace {

vector<LogicalType> TwoIntegers() {
	return {LogicalType::INTEGER, LogicalType::INTEGER};
}

SeamFill Split(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment,
               const SeamObstacle &stop = SeamObstacle()) {
	return FillSeamFromFeed(std::move(feed), fragment, StubSource(), stop, TestIndices());
}

const char *const READ_OF_THE_SOURCE = "proj{id, amt} | crossing[proj{id, amt} | scan]";

} // namespace

TEST_CASE("rows the target holds stay on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | rows");
	REQUIRE(split.obstacle == "the feed reads nothing of the source");
}

TEST_CASE("an operator a source stacks on the seam stays above the rows", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment);

	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
}

TEST_CASE("work the source could compute over rows stays with the rows", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | filter{true} | proj{c0, c1} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | filter{true} | proj{c0, c1} | rows");
	REQUIRE(split.seam_types == TwoIntegers());
	REQUIRE(fragment->seam_types == TwoIntegers());
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
}

TEST_CASE("what fills the seam answers to the seam's index", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	Split(PlanFromDSL(READ_OF_THE_SOURCE), *fragment);

	REQUIRE(!fragment->SeamSlot());
	auto &above = fragment->plan->Cast<LogicalProjection>().expressions[0]->Cast<BoundColumnRefExpression>();
	REQUIRE(fragment->plan->children[0]->GetColumnBindings()[0].table_index == above.Binding().table_index);
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("the fragment still emits what it did before the fill", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto before = fragment->plan->types;

	Split(PlanFromDSL(READ_OF_THE_SOURCE), *fragment);

	REQUIRE(fragment->plan->types == before);
}

TEST_CASE("a read of the same source fills the seam with its own plan", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{id, amt} | filter{amt > 100} | crossing[proj{id, amt} | scan]"), *fragment);

	REQUIRE(!split.remainder);
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | proj{id, amt} | filter{amt > 100} | proj{id, amt} | scan");
}

TEST_CASE("a node the source cannot compute keeps the whole feed on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | filter{target_only(c0)} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | filter{target_only(c0)} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
	REQUIRE(split.obstacle == "filter is not something the source can compute: the memory source has no target_only");
	REQUIRE(split.seam_types == TwoIntegers());
}

TEST_CASE("nothing above a node the source cannot compute crosses", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split =
	    Split(PlanFromDSL("proj{c0, c1} | filter{true} | proj{c0, c1} | filter{target_only(c0)} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | filter{true} | proj{c0, c1} | filter{target_only(c0)} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
}

TEST_CASE("a mixed filter over rows stays whole on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());

	auto split = Split(PlanFromDSL("proj{c0, c1} | filter{c0 > 1, target_only(c0)} | rows"), *fragment);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | filter{c0 > 1, target_only(c0)} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
}

TEST_CASE("a read of another source keeps the whole feed on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	auto feed = Build(Proj("id", "amt"), Filter(Gt(Col("amt"), Int(100))), ScanOf("elsewhere"));

	auto split = Split(std::move(feed), *fragment);

	REQUIRE_PLAN(split.remainder, "proj{id, amt} | filter{amt > 100} | crossing[proj{id, amt} | scan]");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
	REQUIRE(split.obstacle == "a read of another source stays on the target");
}

TEST_CASE("the seam keeps its shape when the feed stays", "[split]") {
	auto fragment = FragmentOverSeam({LogicalType::INTEGER});

	auto split = Split(PlanFromDSL("proj{c0} | filter{target_only(c0)} | rows"), *fragment);

	REQUIRE(split.seam_types == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE(fragment->seam_types == vector<LogicalType> {LogicalType::INTEGER});
	REQUIRE_FRAGMENT(*fragment, "proj{c0} | seam");
	REQUIRE_NOTHROW(fragment->VerifyInvariants());
}

TEST_CASE("a feed that stays must emit the seam's row", "[split]") {
	auto fragment = FragmentOverSeam({LogicalType::INTEGER});

	REQUIRE_THROWS_AS(Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment), InternalException);
}

TEST_CASE("a stop keeps the whole feed on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	SeamObstacle stop;
	stop.stays = true;
	stop.obstacle = "the rows are wanted back";

	auto split = Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment, stop);

	REQUIRE_PLAN(split.remainder, "proj{c0, c1} | rows");
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
	REQUIRE(split.obstacle == "the rows are wanted back");
	REQUIRE(split.seam_types == TwoIntegers());
}

TEST_CASE("a stop keeps a read of the same source on the target", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	SeamObstacle stop;
	stop.stays = true;

	auto split = Split(PlanFromDSL(READ_OF_THE_SOURCE), *fragment, stop);

	REQUIRE_PLAN(split.remainder, READ_OF_THE_SOURCE);
	REQUIRE_FRAGMENT(*fragment, "proj{c0, c1} | seam");
}

TEST_CASE("a stop of a row the seam does not take is refused", "[split]") {
	auto fragment = FragmentOverSeam({LogicalType::INTEGER});
	SeamObstacle stop;
	stop.stays = true;

	REQUIRE_THROWS_AS(Split(PlanFromDSL("proj{c0, c1} | rows"), *fragment, stop), InternalException);
}

TEST_CASE("a fragment with no seam cannot be split into", "[split]") {
	auto fragment = FragmentOverSeam(TwoIntegers());
	Split(PlanFromDSL(READ_OF_THE_SOURCE), *fragment);

	REQUIRE_THROWS_AS(Split(PlanFromDSL(READ_OF_THE_SOURCE), *fragment), InternalException);
}
