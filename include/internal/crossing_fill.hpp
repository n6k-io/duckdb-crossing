#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

//! Turns every update and delete that reads nothing of the rows it writes into one that names no key.
//! Runs before any fold, while such a write still sits directly on its own table's scan.
void FillBareWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                    const CrossingIdentity &identity);

//! Fills every write's seam from its feed: a fence where the feed crosses whole, otherwise a seam
//! insert of the remainder.
void FillWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                const CrossingIdentity &identity);

CrossingVerb VerbOf(LogicalOperator &op);

bool ReturnsRows(LogicalOperator &op);

idx_t TableIndexOf(LogicalOperator &op);

vector<string> SetColumnsOf(LogicalOperator &op, TableCatalogEntry &table);

} // namespace duckdb
