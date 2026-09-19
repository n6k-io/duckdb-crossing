#include "internal/labelling.hpp"

#include "internal/carrier.hpp"
#include "internal/rules.hpp"

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

SubtreeLabel LabelSubtree(LogicalOperator &op, SubtreeLabels &out, optional_ptr<CrossingSource> fence_source,
                          const CrossingIdentity &identity) {
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

	// Every child is labelled before any of them is judged: a branch that disagrees stops this node
	// from crossing, but the branches themselves keep whatever answers they had.
	SubtreeLabel agreed;
	bool agrees = !op.children.empty();
	vector<reference<LogicalOperator>> unasked;
	for (auto &child : op.children) {
		auto child_label = LabelSubtree(*child, out, fence_source, identity);
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
		if (agreed.source && agreed.source != child_label.source) {
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
	if (asked && !NodeCanCross(op, *asked)) {
		return {};
	}

	out[&op] = agreed;
	return agreed;
}

} // namespace

SubtreeLabels LabelSubtrees(LogicalOperator &plan, const CrossingIdentity &identity) {
	SubtreeLabels out;
	LabelSubtree(plan, out, nullptr, identity);
	return out;
}

SubtreeLabels LabelSubtreesUnder(LogicalOperator &feed, CrossingSource &source) {
	SubtreeLabels out;
	LabelSubtree(feed, out, &source, source.Identity());
	return out;
}

} // namespace duckdb
