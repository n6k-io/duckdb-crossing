#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

using FreshTableIndex = std::function<idx_t()>;

unordered_map<idx_t, idx_t> RemapTableIndices(LogicalOperator &op, const FreshTableIndex &fresh);

} // namespace duckdb
