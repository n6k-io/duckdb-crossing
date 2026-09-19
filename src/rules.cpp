#include "internal/rules.hpp"

#include "internal/source_evaluation.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"

namespace duckdb {

namespace {

//! A percentage is of the source's row count, which is not the count the target would have taken.
bool BoundCrosses(const BoundLimitNode &node) {
	switch (node.Type()) {
	case LimitNodeType::UNSET:
	case LimitNodeType::CONSTANT_VALUE:
		return true;
	case LimitNodeType::EXPRESSION_VALUE:
		return IsColumnFreeExpression(node.GetValueExpression());
	default:
		return false;
	}
}

} // namespace

RuleKind RuleKindFor(LogicalOperatorType type) {
	switch (type) {
	// Reshaping the child's output is no obstacle: the scan above a crossed subtree is rebuilt from
	// what that subtree emits, whatever shape it is.
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_SAMPLE:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_PIVOT:
	// Two branches are no obstacle either: the labelling asks each separately, and they cross
	// together only if they name one source.
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return RuleKind::EVALUABLE;
	case LogicalOperatorType::LOGICAL_LIMIT:
		return RuleKind::LIMIT;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_TOP_N:
		return RuleKind::ORDER;
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
	case LogicalOperatorType::LOGICAL_COPY_DATABASE:
	case LogicalOperatorType::LOGICAL_EXPORT:
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_VACUUM:
	case LogicalOperatorType::LOGICAL_ATTACH:
	case LogicalOperatorType::LOGICAL_DETACH:
	case LogicalOperatorType::LOGICAL_LOAD:
	case LogicalOperatorType::LOGICAL_UPDATE_EXTENSIONS:
	case LogicalOperatorType::LOGICAL_SET:
	case LogicalOperatorType::LOGICAL_RESET:
	case LogicalOperatorType::LOGICAL_PRAGMA:
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_PREPARE:
	case LogicalOperatorType::LOGICAL_EXECUTE:
	case LogicalOperatorType::LOGICAL_CREATE_SECRET:
	case LogicalOperatorType::LOGICAL_EXPLAIN:
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		return RuleKind::NEVER;
	default:
		return RuleKind::NONE;
	}
}

bool IsMaterialisedRows(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_CHUNK_GET || type == LogicalOperatorType::LOGICAL_EXPRESSION_GET;
}

//! A limit's bound is not in `expressions`, so it is checked here rather than by CanEvaluateAll.
bool LimitBoundsCross(LogicalOperator &op) {
	auto &limit = op.Cast<LogicalLimit>();
	return BoundCrosses(limit.limit_val) && BoundCrosses(limit.offset_val);
}

const vector<BoundOrderByNode> &OrdersOf(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_TOP_N) {
		return op.Cast<LogicalTopN>().orders;
	}
	return op.Cast<LogicalOrder>().orders;
}

CrossingVerdict NodeVerdict(LogicalOperator &op, CrossingSource &source) {
	auto name = StringUtil::Lower(LogicalOperatorToString(op.type));
	switch (RuleKindFor(op.type)) {
	case RuleKind::EVALUABLE:
		break;
	case RuleKind::LIMIT:
		if (!LimitBoundsCross(op)) {
			return CrossingVerdict::No("the limit's bound is not a constant");
		}
		break;
	case RuleKind::ORDER:
		for (auto &order : OrdersOf(op)) {
			if (!order.expression) {
				return CrossingVerdict::No("the sort has no key");
			}
			auto key = VerdictOn(*order.expression, source);
			if (!key.ok) {
				return key;
			}
		}
		break;
	default:
		return CrossingVerdict::No("crossing does not move a " + name);
	}
	auto expressions = VerdictOnAll(op, source);
	if (!expressions.ok) {
		return expressions;
	}
	return source.AcceptsOperator(op);
}

bool NodeCanCross(LogicalOperator &op, CrossingSource &source) {
	return NodeVerdict(op, source).ok;
}

} // namespace duckdb
