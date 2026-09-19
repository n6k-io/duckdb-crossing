#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

//! Fills every write's seam from its feed: a fence where the feed crosses whole, otherwise a seam
//! insert of the remainder.
void FillWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                const CrossingIdentity &identity);

CrossingVerb VerbOf(LogicalOperator &op);

bool ReturnsRows(LogicalOperator &op);

idx_t TableIndexOf(LogicalOperator &op);

vector<string> SetColumnsOf(LogicalOperator &op, TableCatalogEntry &table);

void MaterialiseConstantRows(ClientContext &context, unique_ptr<LogicalOperator> &node);

} // namespace duckdb
