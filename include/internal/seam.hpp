#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

constexpr const char *CROSSING_SEAM_FUNCTION = "crossing_seam";
constexpr const char *CROSSING_WRITE_NODE_FUNCTION = "crossing_write_node";

//! `types` is key_columns then set_columns.
struct CrossingSeam {
	vector<string> key_columns;
	vector<string> set_columns;
	vector<LogicalType> types;
	vector<string> returning_columns;
	vector<LogicalType> returning_types;
};

TableFunction CrossingSeamFunction();
TableFunction CrossingWriteNodeFunction();

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types);

//! `feed` may be null: a write that takes nothing.
unique_ptr<LogicalOperator> MakeWriteNode(idx_t table_index, CrossingWriteNode node, unique_ptr<LogicalOperator> feed);

unique_ptr<LogicalOperator> *FindSeamSlot(unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
