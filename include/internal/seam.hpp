#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

constexpr const char *CROSSING_SEAM_FUNCTION = "crossing_seam";

TableFunction CrossingSeamFunction();

unique_ptr<LogicalOperator> *FindSeamSlot(unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
