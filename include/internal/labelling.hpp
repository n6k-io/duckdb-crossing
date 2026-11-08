#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

//! Per node, the source its whole subtree could run on. A node is absent when its subtree spans two
//! groups, holds something that cannot cross, or reaches a leaf that is not a crossing scan.
//! A subtree of nothing but rows the target holds is labelled with no source at all: it can run
//! wherever the subtree above it goes.
struct SubtreeLabel {
	CrossingSource *source = nullptr;
	bool runs_anywhere = false;
	//! CTEs a ref inside names but nothing inside defines. A subtree with any is never folded on its own.
	set<TableIndex> open;
	//! A scan inside already takes rows from the target. One crossing takes one seam, so two never merge.
	bool holds_seam = false;
};

using SubtreeLabels = unordered_map<LogicalOperator *, SubtreeLabel>;

SubtreeLabels LabelSubtrees(LogicalOperator &plan, const CrossingIdentity &identity, bool single_joins_cross = true);

SubtreeLabels LabelSubtreesUnder(LogicalOperator &feed, CrossingSource &source, bool single_joins_cross = true);

} // namespace duckdb
