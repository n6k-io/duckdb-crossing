#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

//! Folds a subtree that can run wholly on one source into the fragment of the crossing scan inside
//! it, and hands back that scan to stand in the subtree's place.
//!
//! The caller decides the subtree is crossable -- see LabelSubtrees. This only moves it.
unique_ptr<LogicalOperator> FoldSubtreeIntoItsFragment(unique_ptr<LogicalOperator> subtree,
                                                       const CrossingIdentity &identity);

void AdoptSeamIndex(LogicalOperator &feed, idx_t seam_index);

bool SubtreeHoldsFrozenScan(LogicalOperator &node, const CrossingIdentity &identity);

void UnfoldScansIntoPlace(unique_ptr<LogicalOperator> &subtree, const CrossingIdentity &identity);

} // namespace duckdb
