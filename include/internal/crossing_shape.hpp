#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "internal/table_indices.hpp"

namespace duckdb {

void ShapeWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh);

} // namespace duckdb
