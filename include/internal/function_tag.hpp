#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//! Unique to each compiled copy of crossing. Another copy's table functions can share names and bind
//! data class names with this one, so only this pointer tells them apart without reading their bind data.
TableFunction &TagCrossingFunction(TableFunction &function);
bool IsOwnCrossingFunction(const LogicalGet &get);

} // namespace duckdb
