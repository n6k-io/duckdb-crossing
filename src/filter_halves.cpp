#include "internal/filter_halves.hpp"

#include "internal/carrier.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

namespace {

//! Which source a filter is split against: the first crossing scan beneath it. A filter with none
//! below has nothing to cross into and is left whole.
optional_ptr<CrossingSource> SourceOfFirstScanBelow(LogicalOperator &op, const CrossingIdentity &identity) {
	if (auto source = CrossingSourceOf(op, identity)) {
		return source;
	}
	for (auto &child : op.children) {
		if (auto found = SourceOfFirstScanBelow(*child, identity)) {
			return found;
		}
	}
	return nullptr;
}

void SplitOneFilter(unique_ptr<LogicalOperator> &node, optional_ptr<CrossingSource> against, bool evaluable_above,
                    const CrossingIdentity &identity) {
	auto &filter = node->Cast<LogicalFilter>();
	if (filter.expressions.size() < 2 || filter.children.size() != 1) {
		return;
	}
	auto source = against ? against : SourceOfFirstScanBelow(*filter.children[0], identity);
	if (!source) {
		return;
	}

	vector<unique_ptr<Expression>> evaluable;
	vector<unique_ptr<Expression>> kept;
	for (auto &expr : filter.expressions) {
		if (CanEvaluate(*expr, *source)) {
			evaluable.push_back(std::move(expr));
		} else {
			kept.push_back(std::move(expr));
		}
	}
	if (evaluable.empty() || kept.empty()) {
		filter.expressions.clear();
		for (auto &expr : evaluable) {
			filter.expressions.push_back(std::move(expr));
		}
		for (auto &expr : kept) {
			filter.expressions.push_back(std::move(expr));
		}
		return;
	}

	auto &upper_half = evaluable_above ? evaluable : kept;
	auto &lower_half = evaluable_above ? kept : evaluable;

	auto lower = make_uniq<LogicalFilter>();
	lower->expressions = std::move(lower_half);
	lower->children.push_back(std::move(filter.children[0]));
	lower->ResolveOperatorTypes();

	filter.expressions = std::move(upper_half);
	filter.children[0] = std::move(lower);
	filter.ResolveOperatorTypes();
}

void PushOneFilter(unique_ptr<LogicalOperator> &node, const CrossingIdentity &identity) {
	auto &filter = node->Cast<LogicalFilter>();
	if (filter.children.size() != 1 || !filter.projection_map.empty() || !IsInnerJoin(*filter.children[0])) {
		return;
	}
	auto &join = *filter.children[0];
	vector<vector<unique_ptr<Expression>>> pushed(join.children.size());
	vector<unique_ptr<Expression>> kept;
	for (auto &expr : filter.expressions) {
		bool moved = false;
		bool movable = !expr->IsVolatile() && expr->IsConsistent();
		for (idx_t side = 0; movable && side < join.children.size() && !moved; side++) {
			auto &branch = join.children[side];
			if (!SourceOfFirstScanBelow(*branch, identity) || !BindsOnlyTo(*expr, TableIndicesOf(*branch))) {
				continue;
			}
			pushed[side].push_back(std::move(expr));
			moved = true;
		}
		if (!moved) {
			kept.push_back(std::move(expr));
		}
	}
	for (idx_t side = 0; side < join.children.size(); side++) {
		if (pushed[side].empty()) {
			continue;
		}
		auto below = make_uniq<LogicalFilter>();
		below->expressions = std::move(pushed[side]);
		below->children.push_back(std::move(join.children[side]));
		below->ResolveOperatorTypes();
		join.children[side] = std::move(below);
	}
	filter.expressions = std::move(kept);
	if (filter.expressions.empty()) {
		node = std::move(filter.children[0]);
	}
}

} // namespace

void PushFiltersIntoJoinBranches(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity) {
	for (auto &child : plan->children) {
		PushFiltersIntoJoinBranches(child, identity);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		PushOneFilter(plan, identity);
	}
}

void SplitFiltersAtEvaluableHalf(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity) {
	for (auto &child : plan->children) {
		SplitFiltersAtEvaluableHalf(child, identity);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		SplitOneFilter(plan, nullptr, false, identity);
	}
}

void SplitFiltersAgainst(unique_ptr<LogicalOperator> &plan, CrossingSource &source) {
	for (auto &child : plan->children) {
		SplitFiltersAgainst(child, source);
	}
	if (plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		SplitOneFilter(plan, &source, true, source.Identity());
	}
}

void RejoinAdjacentFiltersEverywhere(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity) {
	for (auto &child : plan->children) {
		RejoinAdjacentFiltersEverywhere(child, identity);
	}
	if (auto fragment = CrossingReadFragmentOf(*plan, identity)) {
		if (fragment->plan) {
			RejoinAdjacentFiltersEverywhere(fragment->plan, identity);
		}
	}
	while (RejoinOneFilter(*plan)) {
	}
}

//! Only a filter directly above another rejoins -- that is the shape the split leaves behind, and
//! anything standing between two filters may be reading what the lower one emits.
bool RejoinOneFilter(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_FILTER || op.children.size() != 1) {
		return false;
	}
	auto &child = *op.children[0];
	if (child.type != LogicalOperatorType::LOGICAL_FILTER) {
		return false;
	}
	if (!child.Cast<LogicalFilter>().projection_map.empty()) {
		return false;
	}

	auto &filter = op.Cast<LogicalFilter>();
	auto &below = child.Cast<LogicalFilter>();
	for (auto &expr : below.expressions) {
		filter.expressions.push_back(std::move(expr));
	}
	op.children[0] = std::move(below.children[0]);
	op.ResolveOperatorTypes();
	return true;
}

bool IsInnerJoin(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return true;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return op.Cast<LogicalJoin>().join_type == JoinType::INNER;
	default:
		return false;
	}
}

set<idx_t> TableIndicesOf(LogicalOperator &op) {
	set<idx_t> out;
	for (auto &binding : op.GetColumnBindings()) {
		out.insert(binding.table_index);
	}
	return out;
}

bool BindsOnlyTo(const Expression &expr, const set<idx_t> &tables) {
	bool within = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { within = within && BindsOnlyTo(child, tables); });
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		within = within && tables.count(expr.Cast<BoundColumnRefExpression>().binding.table_index) > 0;
	}
	return within;
}

} // namespace duckdb
