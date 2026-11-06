#pragma once

#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/fragment.hpp"

namespace duckdb {

struct CrossingFeed {
	unique_ptr<LogicalOperator> plan;
	//! Set when `plan` emits the row image RETURNING needs: where each seam column sits in that image.
	vector<idx_t> seam_positions;
};

CrossingFeed FeedOf(LogicalOperator &write, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                    const FreshTableIndex &fresh);

struct SeamObstacle {
	bool stays = false;
	string obstacle;
	vector<idx_t> seam_positions;
};

struct SeamFill {
	unique_ptr<LogicalOperator> remainder;
	vector<LogicalType> seam_types;
	string obstacle;
};

SeamFill FillSeamFromFeed(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment, CrossingSource &source,
                          const SeamObstacle &stop, const FreshTableIndex &fresh, bool single_joins_cross = true);

void MaterialiseConstantRows(ClientContext &context, unique_ptr<LogicalOperator> &node, bool only_consistent = false);

//! The default every column of a crossing table declares. Only the source knows the real one, so this
//! throws if it is ever executed, and an insert drops the columns holding it.
unique_ptr<ParsedExpression> SourceDefaultExpression();
void RegisterSourceDefaultFunction(DatabaseInstance &db);
bool IsSourceDefault(const Expression &expr);

} // namespace duckdb
