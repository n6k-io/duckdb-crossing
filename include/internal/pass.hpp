#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

//! Moves everything that can cross to the source: every subtree LabelSubtrees gives a source folds
//! into a crossing scan, unless it holds a frozen scan.
//!
//! Takes a plan duckdb's optimizers have not run on: it is still one node per operation, with nothing
//! folded into a scan and nothing yet moved for the target's benefit. What crosses is optimized by
//! whoever runs it, what stays behind by duckdb afterwards.
//! With `seams`, an inner join left between a scan and rows that read nothing of any source then runs on
//! the source, taking those rows through a seam, when a condition it accepts links the two sides and it
//! accepts the seam. This follows the plan's shape and never table sizes.
void FoldCrossableWorkIntoFragments(unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                                    const CrossingIdentity &identity, bool single_joins_cross = true,
                                    bool seams = true);

bool PlanHoldsCrossing(LogicalOperator &op, const CrossingIdentity &identity);

//! Wire into the optimizer's post hook: it runs after duckdb has dropped the columns nobody reads.
void NarrowScansToRequestedColumns(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity);

} // namespace duckdb
