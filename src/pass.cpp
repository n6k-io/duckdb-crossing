#include "internal/pass.hpp"

#include "internal/carrier.hpp"
#include "internal/filter_halves.hpp"
#include "internal/fold.hpp"
#include "internal/labelling.hpp"
#include "internal/scan_columns.hpp"

#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

//! Top-down, so the first labelled node reached is the largest one: everything below it is labelled
//! too, and folding the biggest is what leaves the least behind.
void FoldEveryLabelledSubtree(unique_ptr<LogicalOperator> &node, const SubtreeLabels &sources,
                              const CrossingIdentity &identity) {
	auto label = sources.find(node.get());
	if (label != sources.end() && label->second.source && !SubtreeHoldsFrozenScan(*node, identity)) {
		node = FoldSubtreeIntoItsFragment(std::move(node), identity);
		return;
	}
	for (auto &child : node->children) {
		FoldEveryLabelledSubtree(child, sources, identity);
	}
}

//! Before anything is asked about crossing, so a fragment describes the columns its scan wants now
//! rather than the ones it wanted when it was bound.
void NarrowEveryFragmentToItsScan(LogicalOperator &node, const FreshTableIndex &fresh,
                                  const CrossingIdentity &identity) {
	if (auto fragment = CrossingReadFragmentOf(node, identity)) {
		if (!fragment->frozen) {
			fragment->RemapTableIndices(fresh);
			NarrowFragmentToScan(node.Cast<LogicalGet>(), *fragment);
		}
	}
	for (auto &child : node.children) {
		NarrowEveryFragmentToItsScan(*child, fresh, identity);
	}
}

bool PlanHoldsCrossing(LogicalOperator &op, const CrossingIdentity &identity) {
	if (CrossingReadFragmentOf(op, identity)) {
		return true;
	}
	for (auto &child : op.children) {
		if (PlanHoldsCrossing(*child, identity)) {
			return true;
		}
	}
	return false;
}

} // namespace

void NarrowScansToRequestedColumns(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity) {
	if (auto fragment = CrossingReadFragmentOf(*plan, identity)) {
		if (!fragment->frozen) {
			NarrowFragmentAndScanToRequestedColumns(plan->Cast<LogicalGet>(), *fragment);
		}
	}
	for (auto &child : plan->children) {
		NarrowScansToRequestedColumns(child, identity);
	}
}

void FoldCrossableWorkIntoFragments(unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                                    const CrossingIdentity &identity) {
	if (!PlanHoldsCrossing(*plan, identity)) {
		return;
	}
	NarrowEveryFragmentToItsScan(*plan, fresh, identity);
	PushFiltersIntoJoinBranches(plan, identity);
	SplitFiltersAtEvaluableHalf(plan, identity);
	auto sources = LabelSubtrees(*plan, identity);
	FoldEveryLabelledSubtree(plan, sources, identity);
	RejoinAdjacentFiltersEverywhere(plan, identity);
}

} // namespace duckdb
