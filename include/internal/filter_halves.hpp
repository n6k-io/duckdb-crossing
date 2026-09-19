#pragma once

#include "duckdb/common/set.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

//! Divides each filter into the conjuncts the source below it can compute and the ones it cannot,
//! putting the computable half nearest the crossing. A filter crosses whole or not at all, so without
//! this split one unevaluable conjunct keeps the rest from crossing.
void SplitFiltersAtEvaluableHalf(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity);

void PushFiltersIntoJoinBranches(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity);

void SplitFiltersAgainst(unique_ptr<LogicalOperator> &plan, CrossingSource &source);

//! The inverse of the split, for halves that ended up on the same side.
void RejoinAdjacentFiltersEverywhere(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity);

bool RejoinOneFilter(LogicalOperator &op);

bool IsInnerJoin(LogicalOperator &op);

set<idx_t> TableIndicesOf(LogicalOperator &op);

bool BindsOnlyTo(const Expression &expr, const set<idx_t> &tables);

} // namespace duckdb
