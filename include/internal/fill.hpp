#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/fragment.hpp"

namespace duckdb {

struct CrossingFeed {
	unique_ptr<LogicalOperator> plan;
	optional_ptr<LogicalOperator> image;
};

CrossingFeed FeedOf(LogicalOperator &write, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                    const FreshTableIndex &fresh);

struct SeamObstacle {
	optional_ptr<LogicalOperator> node;
	string obstacle;
};

struct SeamFill {
	unique_ptr<LogicalOperator> remainder;
	vector<LogicalType> seam_types;
	string obstacle;
};

SeamFill FillSeamFromFeed(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment, CrossingSource &source,
                          const SeamObstacle &stop, const FreshTableIndex &fresh);

} // namespace duckdb
