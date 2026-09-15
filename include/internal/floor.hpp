#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

constexpr const char *CROSSING_FLOOR_FUNCTION = "crossing_floor";

TableFunction CrossingFloorFunction();

} // namespace duckdb
