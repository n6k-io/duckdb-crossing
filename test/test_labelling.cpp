#include "internal/fold.hpp"
#include "framework/plan_builder.hpp"
#include "framework/stub_seam.hpp"
#include "internal/labelling.hpp"
#include "internal/table_indices.hpp"

#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

using namespace duckdb;

namespace {

bool IsLabelled(const SubtreeLabels &labels, LogicalOperator &op) {
	return labels.find(&op) != labels.end();
}

string LabelOf(const SubtreeLabels &labels, LogicalOperator &op) {
	auto found = labels.find(&op);
	if (found == labels.end()) {
		return string();
	}
	for (auto &name : {"memory", "elsewhere"}) {
		if (found->second.source == &MemorySourceFor(name, {})) {
			return name;
		}
	}
	return found->second.runs_anywhere ? "*" : "?";
}

unique_ptr<LogicalOperator> RefTo(TableIndex cte) {
	return make_uniq<LogicalCTERef>(TableIndex(90), cte, vector<LogicalType> {LogicalType::INTEGER},
	                                vector<Identifier> {Identifier("id")});
}

unique_ptr<LogicalOperator> CteOver(TableIndex cte, unique_ptr<LogicalOperator> definition,
                                    unique_ptr<LogicalOperator> body) {
	return make_uniq<LogicalMaterializedCTE>(Identifier("x"), cte, 1, std::move(definition), std::move(body),
	                                         CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
}

} // namespace

TEST_CASE("a CTE takes its definition's source, and its ref is labelled only within it", "[labelling]") {
	auto ref = RefTo(TableIndex(91));
	auto &ref_node = *ref;
	auto plan = CteOver(TableIndex(91), PlanFromDSL("crossing[proj{id, amt} | scan]"), std::move(ref));

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(LabelOf(labels, *plan) == "memory");
	REQUIRE(!IsLabelled(labels, ref_node));
}

TEST_CASE("remapping a CTE keeps its refs pointing at it", "[labelling]") {
	auto ref = RefTo(TableIndex(91));
	auto &ref_node = ref->Cast<LogicalCTERef>();
	auto plan = CteOver(TableIndex(91), PlanFromDSL("crossing[proj{id, amt} | scan]"), std::move(ref));

	RemapTableIndices(*plan, TestIndices());

	REQUIRE(ref_node.cte_index == plan->Cast<LogicalCTE>().table_index);
	REQUIRE(ref_node.cte_index != TableIndex(91));
}

TEST_CASE("a ref with no definition in the plan is not labelled", "[labelling]") {
	auto plan = RefTo(TableIndex(91));

	REQUIRE(LabelSubtrees(*plan, MemoryIdentity()).empty());
}

TEST_CASE("a CTE whose body stays leaves its definition to cross alone", "[labelling]") {
	auto definition = PlanFromDSL("crossing[proj{id, amt} | scan]");
	auto &definition_node = *definition;
	auto plan = CteOver(TableIndex(91), std::move(definition), PlanFromDSL("local"));

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, definition_node) == "memory");
}

TEST_CASE("a crossing scan names its own source", "[labelling]") {
	auto plan = PlanFromDSL("crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(LabelOf(labels, *plan) == "memory");
}

TEST_CASE("a leaf that is not a crossing scan names nothing", "[labelling]") {
	auto plan = PlanFromDSL("local");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(labels.empty());
}

TEST_CASE("a filter the source can compute takes the source's name", "[labelling]") {
	auto plan = PlanFromDSL("filter{amt > 100} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(LabelOf(labels, *plan) == "memory");
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("a filter the source cannot compute is unnamed, and the scan below it is not", "[labelling]") {
	auto plan = PlanFromDSL("filter{target_only(amt)} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("an order the source can compute takes the source's name", "[labelling]") {
	auto plan = PlanFromDSL("order{amt} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(LabelOf(labels, *plan) == "memory");
}

TEST_CASE("an order whose keys the source cannot compute is unnamed, and the scan below it is not", "[labelling]") {
	auto plan = PlanFromDSL("order{target_only(amt)} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("a join of two scans of one source takes that source's name", "[labelling]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], crossing[proj{id, amt} | scan])");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(LabelOf(labels, *plan) == "memory");
}

TEST_CASE("a join of two different sources is unnamed, and both scans keep theirs", "[labelling]") {
	auto plan = Build(Join(ScanOf("memory"), ScanOf("elsewhere")));

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
	REQUIRE(LabelOf(labels, *plan->children[1]) == "elsewhere");
}

TEST_CASE("a join with one branch on the target is unnamed", "[labelling]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], local)");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(LabelOf(labels, *plan->children[0]) == "memory");
}

TEST_CASE("an unnamed node stops everything above it being named", "[labelling]") {
	auto plan = PlanFromDSL("filter{amt > 100} | filter{target_only(amt)} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(!IsLabelled(labels, *plan));
	REQUIRE(!IsLabelled(labels, *plan->children[0]));
	REQUIRE(LabelOf(labels, *plan->children[0]->children[0]) == "memory");
}

TEST_CASE("what the labelling names is what the fold takes", "[labelling]") {
	auto plan = PlanFromDSL("limit{5} | filter{amt > 100} | crossing[proj{id, amt} | scan]");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());
	REQUIRE(LabelOf(labels, *plan) == "memory");

	plan = FoldSubtreeIntoItsFragment(std::move(plan), MemoryIdentity());

	REQUIRE_PLAN(plan, "crossing[limit{5} | filter{amt > 100} | proj{id, amt} | scan]");
}

TEST_CASE("rows the target holds run anywhere beneath a write", "[labelling]") {
	auto plan = PlanFromDSL("proj{c0, c1} | rows");

	auto labels = LabelSubtreesUnder(*plan, StubSource());

	REQUIRE(LabelOf(labels, *plan) == "*");
	REQUIRE(LabelOf(labels, *plan->children[0]) == "*");
}

TEST_CASE("rows the target holds run anywhere outside a write too", "[labelling]") {
	auto plan = PlanFromDSL("proj{c0, c1} | rows");

	auto labels = LabelSubtrees(*plan, MemoryIdentity());

	REQUIRE(LabelOf(labels, *plan) == "*");
	REQUIRE(LabelOf(labels, *plan->children[0]) == "*");
}

TEST_CASE("rows beside a scan take the scan's source", "[labelling]") {
	auto plan = PlanFromDSL("join(crossing[proj{id, amt} | scan], rows)");

	auto under_write = LabelSubtreesUnder(*plan, StubSource());
	REQUIRE(LabelOf(under_write, *plan) == "memory");

	auto under_read = LabelSubtrees(*plan, MemoryIdentity());
	REQUIRE(LabelOf(under_read, *plan) == "memory");
}

TEST_CASE("what stands over the rows is asked of the source they meet", "[labelling]") {
	auto crosses = PlanFromDSL("join(crossing[proj{id, amt} | scan], filter{c0 > 1} | rows)");
	auto stays = PlanFromDSL("join(crossing[proj{id, amt} | scan], filter{target_only(c0)} | rows)");

	REQUIRE(LabelOf(LabelSubtrees(*crosses, MemoryIdentity()), *crosses) == "memory");
	REQUIRE(!IsLabelled(LabelSubtrees(*stays, MemoryIdentity()), *stays));
}
