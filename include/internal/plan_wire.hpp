#pragma once

namespace duckdb {

class DatabaseInstance;

//! Makes crossing_floor and crossing_seam resolvable by name, which DeserializeCrossingPlan needs.
void RegisterCrossingPlanFunctions(DatabaseInstance &db);

} // namespace duckdb
