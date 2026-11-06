#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

//! A crossing write inside a CTE is refused: DuckDB could drop the near node, not seeing a write in it.
void RefuseWritesInCtes(LogicalOperator &plan, const CrossingIdentity &identity);

//! Drops what an update or merge only sets to itself, which DuckDB adds so a RETURNING has the whole row.
void NarrowSetsToWhatChanges(LogicalOperator &plan, const CrossingIdentity &identity);

//! Drops the columns an insert did not name from its row and records them in `column_index_map`, so the
//! source applies its own defaults.
void LeaveInsertDefaultsToSource(LogicalOperator &plan, const CrossingIdentity &identity);

//! Turns every update and delete that reads nothing of the rows it writes into one that names no key.
//! Runs before any fold, while such a write still sits directly on its own table's scan.
void FillBareWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                    const CrossingIdentity &identity);

//! Fills every write's seam from its feed: a near node with no child where the feed crosses whole,
//! otherwise one over the remainder.
void FillWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                const CrossingIdentity &identity);

CrossingVerb VerbOf(LogicalOperator &op);

bool ReturnsRows(LogicalOperator &op);

TableIndex TableIndexOf(LogicalOperator &op);

vector<string> SetColumnsOf(LogicalOperator &op, TableCatalogEntry &table);

} // namespace duckdb
