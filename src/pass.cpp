#include "internal/pass.hpp"

#include "internal/carrier.hpp"
#include "internal/filter_halves.hpp"
#include "internal/fold.hpp"
#include "internal/crossing_read.hpp"
#include "internal/labelling.hpp"
#include "internal/rules.hpp"
#include "internal/scan_columns.hpp"
#include "internal/seam.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

//! Top-down, so the first labelled node reached is the largest one: folding the biggest is what leaves
//! the least behind.
void FoldEveryLabelledSubtree(unique_ptr<LogicalOperator> &node, const SubtreeLabels &sources,
                              const CrossingIdentity &identity, const FreshTableIndex &fresh,
                              vector<ReplacementBinding> &rewired) {
	auto label = sources.find(node.get());
	if (label != sources.end() && label->second.source && !SubtreeHoldsFrozenScan(*node, identity)) {
		node = FoldSubtreeIntoItsFragment(std::move(node), identity, fresh, rewired);
		return;
	}
	for (auto &child : node->children) {
		FoldEveryLabelledSubtree(child, sources, identity, fresh, rewired);
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

} // namespace

namespace {

struct SeamCuts {
	vector<ReplacementBinding> rewired;
	vector<pair<reference<LogicalOperator>, unique_ptr<LogicalOperator>>> rows;
};

//! Reads no source, of this identity or any other, and needs nothing from the plan around it.
bool StandsAlone(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return false;
	case LogicalOperatorType::LOGICAL_GET:
		if (op.Cast<LogicalGet>().function.GetName() == CROSSING_READ_FUNCTION) {
			return false;
		}
		break;
	default:
		break;
	}
	for (auto &child : op.children) {
		if (!StandsAlone(*child)) {
			return false;
		}
	}
	return true;
}

bool LinksBothSides(LogicalFilter &filter, LogicalOperator &join, CrossingSource &source) {
	auto left = TableIndicesOf(*join.children[0]);
	auto right = TableIndicesOf(*join.children[1]);
	bool linked = false;
	for (auto &expr : filter.expressions) {
		if (!CanEvaluate(*expr, source)) {
			return false;
		}
		linked = linked || (!BindsOnlyTo(*expr, left) && !BindsOnlyTo(*expr, right));
	}
	return linked;
}

//! `node` is an inner join with conditions, or a cross product under the filter holding them.
void CutOneJoin(unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh, const CrossingIdentity &identity,
                SeamCuts &cuts) {
	bool under_filter = node->type == LogicalOperatorType::LOGICAL_FILTER && node->children.size() == 1 &&
	                    node->children[0]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
	                    node->Cast<LogicalFilter>().projection_map.empty();
	auto &join = under_filter ? *node->children[0] : *node;
	if (!under_filter && (join.type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT || !IsInnerJoin(join))) {
		return;
	}
	if (join.children.size() != 2) {
		return;
	}
	optional_idx far_side;
	for (idx_t side = 0; side < 2; side++) {
		auto fragment = CrossingReadFragmentOf(*join.children[side], identity);
		if (fragment && !fragment->frozen && !fragment->HasSeam()) {
			far_side = side;
		}
	}
	if (!far_side.IsValid()) {
		return;
	}
	auto &source = *CrossingSourceOf(*join.children[far_side.GetIndex()], identity);
	auto &near_slot = join.children[1 - far_side.GetIndex()];
	if (IsMaterialisedRows(near_slot->type) || !StandsAlone(*near_slot) || !NodeCanCross(join, source)) {
		return;
	}
	if (under_filter && (!NodeCanCross(*node, source) || !LinksBothSides(node->Cast<LogicalFilter>(), join, source))) {
		return;
	}
	near_slot->ResolveOperatorTypes();
	auto types = near_slot->types;
	auto bindings = near_slot->GetColumnBindings();
	if (!source.AcceptsSeam(types).ok) {
		return;
	}

	auto seam_index = fresh();
	auto rows = std::move(near_slot);
	near_slot = MakeSeamNode(seam_index.index, types);
	vector<ReplacementBinding> onto_seam;
	for (idx_t i = 0; i < bindings.size(); i++) {
		onto_seam.emplace_back(bindings[i], ColumnBinding(seam_index, ProjectionIndex(i)));
	}
	ColumnBindingReplacer replacer;
	replacer.replacement_bindings = onto_seam;
	replacer.VisitOperator(*node);

	CrossingReadFragmentOf(*join.children[far_side.GetIndex()], identity)->seam_types = types;
	vector<ReplacementBinding> folded;
	node = FoldSubtreeIntoItsFragment(std::move(node), identity, fresh, folded);

	// What the plan above calls the rows has to reach the scan in one step, past the seam.
	if (folded.empty()) {
		cuts.rewired.insert(cuts.rewired.end(), onto_seam.begin(), onto_seam.end());
	}
	for (auto &step : folded) {
		if (step.old_binding.table_index == seam_index) {
			cuts.rewired.emplace_back(bindings[step.old_binding.column_index], step.new_binding);
		} else {
			cuts.rewired.push_back(step);
		}
	}
	cuts.rows.emplace_back(*node, std::move(rows));
}

//! Bottom-up, so in `far JOIN a JOIN b` the join naming `a` cuts, and the one above then holds a seam.
//! The rows of a recursive CTE or a correlated subquery are read again per iteration, so neither cuts.
void CutJoinsBelow(unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh, const CrossingIdentity &identity,
                   SeamCuts &cuts) {
	if (node->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
	    node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return;
	}
	for (auto &child : node->children) {
		CutJoinsBelow(child, fresh, identity, cuts);
	}
	CutOneJoin(node, fresh, identity, cuts);
}

void FoldLabelled(unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh, const CrossingIdentity &identity,
                  bool single_joins_cross) {
	auto sources = LabelSubtrees(*plan, identity, single_joins_cross);
	vector<ReplacementBinding> rewired;
	FoldEveryLabelledSubtree(plan, sources, identity, fresh, rewired);
	if (!rewired.empty()) {
		ColumnBindingReplacer replacer;
		replacer.replacement_bindings = std::move(rewired);
		replacer.VisitOperator(*plan);
	}
}

} // namespace

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
                                    const CrossingIdentity &identity, bool single_joins_cross, bool seams) {
	if (!PlanHoldsCrossing(*plan, identity)) {
		return;
	}
	NarrowEveryFragmentToItsScan(*plan, fresh, identity);
	PushFiltersIntoJoinBranches(plan, identity);
	SplitFiltersAtEvaluableHalf(plan, identity);
	FoldLabelled(plan, fresh, identity, single_joins_cross);
	if (seams) {
		SeamCuts cuts;
		CutJoinsBelow(plan, fresh, identity, cuts);
		if (!cuts.rows.empty()) {
			// The rows keep their own bindings, so they stay out of the plan until it is rewired.
			ColumnBindingReplacer replacer;
			replacer.replacement_bindings = std::move(cuts.rewired);
			replacer.VisitOperator(*plan);
			for (auto &cut : cuts.rows) {
				cut.first.get().children.push_back(MakeNearChild(std::move(cut.second), fresh));
			}
			FoldLabelled(plan, fresh, identity, single_joins_cross);
		}
	}
	RejoinAdjacentFiltersEverywhere(plan, identity);
}

} // namespace duckdb
