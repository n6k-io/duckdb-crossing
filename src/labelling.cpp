#include "internal/labelling.hpp"

#include "internal/carrier.hpp"
#include "internal/rules.hpp"

#include "duckdb/common/set.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

namespace {

bool Unlabelled(const SubtreeLabel &label) {
	return !label.source && !label.runs_anywhere;
}

bool SubtreeCrossesTo(LogicalOperator &op, CrossingSource &source) {
	if (IsMaterialisedRows(op.type)) {
		return true;
	}
	if (!NodeCanCross(op, source)) {
		return false;
	}
	for (auto &child : op.children) {
		if (!SubtreeCrossesTo(*child, source)) {
			return false;
		}
	}
	return true;
}

struct LabelRun {
	const CrossingIdentity &identity;
	bool single_joins_cross;
	SubtreeLabels out;
	unordered_map<TableIndex, SubtreeLabel> definitions;
};

bool IsSingleJoin(LogicalOperator &op) {
	return (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	        op.type == LogicalOperatorType::LOGICAL_ANY_JOIN || op.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) &&
	       op.Cast<LogicalJoin>().join_type == JoinType::SINGLE;
}

SubtreeLabel LabelSubtree(LogicalOperator &op, LabelRun &run, optional_ptr<CrossingSource> fence_source) {
	auto &out = run.out;
	auto &identity = run.identity;
	if (CrossingReadFragmentOf(op, identity)) {
		SubtreeLabel here;
		here.source = CrossingSourceOf(op, identity).get();
		out[&op] = here;
		return here;
	}
	if (IsMaterialisedRows(op.type)) {
		SubtreeLabel here;
		here.runs_anywhere = true;
		out[&op] = here;
		return here;
	}

	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = op.Cast<LogicalCTERef>();
		auto definition = run.definitions.find(ref.cte_index);
		if (definition == run.definitions.end() || Unlabelled(definition->second)) {
			return {};
		}
		auto here = definition->second;
		here.open.insert(ref.cte_index);
		return here;
	}

	// Every child is labelled before any of them is judged: a branch that disagrees stops this node
	// from crossing, but the branches themselves keep whatever answers they had.
	SubtreeLabel agreed;
	bool agrees = !op.children.empty();
	vector<reference<LogicalOperator>> unasked;
	set<TableIndex> open;
	for (idx_t c = 0; c < op.children.size(); c++) {
		auto &child = op.children[c];
		auto child_label = LabelSubtree(*child, run, fence_source);
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && c == 0) {
			run.definitions[op.Cast<LogicalCTE>().table_index] = child_label;
		}
		open.insert(child_label.open.begin(), child_label.open.end());
		if (Unlabelled(child_label)) {
			agrees = false;
			continue;
		}
		if (!child_label.source) {
			agreed.runs_anywhere = true;
			if (!fence_source) {
				unasked.emplace_back(*child);
			}
			continue;
		}
		if (agreed.source && agreed.source->Group() != child_label.source->Group()) {
			agrees = false;
			continue;
		}
		agreed.source = child_label.source;
	}
	if (!agrees) {
		return {};
	}
	if (agreed.source) {
		agreed.runs_anywhere = false;
		for (auto &child : unasked) {
			if (!SubtreeCrossesTo(child.get(), *agreed.source)) {
				return {};
			}
		}
	}

	auto asked = agreed.source ? agreed.source : fence_source.get();
	if (RuleKindFor(op.type) == RuleKind::NONE) {
		return {};
	}
	// A SINGLE join crosses only where the target raises the multi-row error, and the source must raise
	// it too: whoever runs the join reads the setting, and the plan does not carry it.
	if (!run.single_joins_cross && IsSingleJoin(op)) {
		return {};
	}
	if (asked && !NodeCanCross(op, *asked)) {
		return {};
	}

	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		open.erase(op.Cast<LogicalCTE>().table_index);
	}
	agreed.open = std::move(open);
	if (agreed.open.empty()) {
		out[&op] = agreed;
	}
	return agreed;
}

} // namespace

SubtreeLabels LabelSubtrees(LogicalOperator &plan, const CrossingIdentity &identity, bool single_joins_cross) {
	LabelRun run {identity, single_joins_cross, {}, {}};
	LabelSubtree(plan, run, nullptr);
	return std::move(run.out);
}

SubtreeLabels LabelSubtreesUnder(LogicalOperator &feed, CrossingSource &source, bool single_joins_cross) {
	LabelRun run {source.Identity(), single_joins_cross, {}, {}};
	LabelSubtree(feed, run, &source);
	return std::move(run.out);
}

} // namespace duckdb
