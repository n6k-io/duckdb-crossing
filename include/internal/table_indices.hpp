#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

using FreshTableIndex = std::function<TableIndex()>;

unordered_map<TableIndex, TableIndex> RemapTableIndices(LogicalOperator &op, const FreshTableIndex &fresh);

} // namespace duckdb
