#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

constexpr const char *CROSSING_FLOOR_FUNCTION = "crossing_floor";

struct CrossingFloorNode {
	string catalog;
	string schema;
	string table;
	vector<string> column_names;
};

struct CrossingTableUse {
	//! Empty unless the floor names one.
	string catalog;
	string schema;
	string table;
	//! Indexes into the table's columns, in the order Describe declared them.
	vector<idx_t> columns;
};

TableFunction CrossingFloorFunction();

unique_ptr<LogicalOperator> MakeFloorNode(idx_t table_index, string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types, string catalog = string());
unique_ptr<LogicalOperator> MakeFloorNode(string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types, string catalog = string());

optional_ptr<const CrossingFloorNode> FloorOf(const LogicalOperator &op);

//! Every floor in `plan`, and every scan of a catalog table, with the columns each is read for.
vector<CrossingTableUse> CrossingTablesOf(const LogicalOperator &plan);

} // namespace duckdb
