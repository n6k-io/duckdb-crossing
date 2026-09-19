#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

//! Moves everything that can cross to the source: every subtree that runs wholly on one source folds
//! into a crossing scan.
//!
//! Wire into the optimizer's pre hook. Running before duckdb's optimizers means the plan is still
//! one node per operation, with nothing folded into a scan and nothing yet moved for the target's
//! benefit: what crosses is optimized by whoever runs it, what stays behind by duckdb afterwards.
void FoldCrossableWorkIntoFragments(unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                                    const CrossingIdentity &identity);

//! Wire into the optimizer's post hook: it runs after duckdb has dropped the columns nobody reads.
void NarrowScansToRequestedColumns(unique_ptr<LogicalOperator> &plan, const CrossingIdentity &identity);

} // namespace duckdb
