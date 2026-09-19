#pragma once

#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

//! One operator type's answer, for that node alone. Whether the subtree beneath it also crosses is
//! the labelling's question. An operator with no rule does not cross. Silence is a refusal, never
//! an oversight.
enum class RuleKind : uint8_t { NONE, EVALUABLE, LIMIT, ORDER, NEVER };

RuleKind RuleKindFor(LogicalOperatorType type);

bool IsMaterialisedRows(LogicalOperatorType type);

bool LimitBoundsCross(LogicalOperator &op);

const vector<BoundOrderByNode> &OrdersOf(LogicalOperator &op);

CrossingVerdict NodeVerdict(LogicalOperator &op, CrossingSource &source);

bool NodeCanCross(LogicalOperator &op, CrossingSource &source);

} // namespace duckdb
