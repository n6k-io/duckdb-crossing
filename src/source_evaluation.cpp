#include "internal/source_evaluation.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

namespace {

//! DuckDB binds these as functions. To a source they stay a cast, a comparison and a between, not calls it could lack.
bool IsEngineForm(const Expression &expr) {
	auto type = expr.GetExpressionType();
	return BoundCastExpression::IsCast(expr) || BoundComparisonExpression::IsComparison(expr) ||
	       type == ExpressionType::COMPARE_BETWEEN || type == ExpressionType::COMPARE_NOT_BETWEEN;
}

} // namespace

CrossingVerdict VerdictOn(const Expression &expr, CrossingSource &source) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		auto type = source.AcceptsType(expr.GetReturnType());
		if (!type.ok) {
			return type;
		}
	}
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_CONJUNCTION:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_UNNEST:
		break;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &bound = expr.Cast<BoundFunctionExpression>();
		if (IsEngineForm(expr)) {
			break;
		}
		if (bound.Function().HasBindLambdaCallback()) {
			return CrossingVerdict::No("crossing does not move a lambda");
		}
		auto call = source.AcceptsCall(expr);
		if (!call.ok) {
			return call;
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_AGGREGATE:
	case ExpressionClass::BOUND_WINDOW: {
		auto call = source.AcceptsCall(expr);
		if (!call.ok) {
			return call;
		}
		break;
	}
	default:
		return CrossingVerdict::No("crossing does not move a " + ExpressionClassToString(expr.GetExpressionClass()));
	}
	if (expr.IsVolatile() || !expr.IsConsistent()) {
		return CrossingVerdict::No(expr.ToString() + " would be evaluated twice");
	}
	CrossingVerdict verdict;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (verdict.ok) {
			verdict = VerdictOn(child, source);
		}
	});
	return verdict;
}

bool CanEvaluate(const Expression &expr, CrossingSource &source) {
	return VerdictOn(expr, source).ok;
}

CrossingVerdict VerdictOnAll(LogicalOperator &op, CrossingSource &source) {
	CrossingVerdict verdict;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *child) {
		if (verdict.ok) {
			verdict = VerdictOn(**child, source);
		}
	});
	return verdict;
}

bool CanEvaluateAll(LogicalOperator &op, CrossingSource &source) {
	return VerdictOnAll(op, source).ok;
}

bool IsColumnFreeExpression(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	bool column_free = true;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { column_free = column_free && IsColumnFreeExpression(child); });
	return column_free;
}

} // namespace duckdb
