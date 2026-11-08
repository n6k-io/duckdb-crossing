#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

unique_ptr<QueryResult> SubmitCrossingPlan(ClientContext &context, unique_ptr<LogicalOperator> plan);

} // namespace duckdb
