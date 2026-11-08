#pragma once

#include "crossing.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! Replaces each table scan in `plan` the resolver polices with a secure view over its policy. The target scan of an
//! UPDATE or DELETE is asked with `verb` and `reads_rows`; every other scan is asked as a SELECT. `catalog` is the one
//! far binds in, named in a floor as empty.
void ApplyRowPolicies(Binder &binder, unique_ptr<LogicalOperator> &plan, const string &catalog,
                      const CrossingFloorResolver &resolver, CrossingVerb verb, bool reads_rows);

} // namespace duckdb
