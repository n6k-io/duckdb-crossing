#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "crossing.hpp"

namespace duckdb {

class CrossingTableCatalogEntry : public TableCatalogEntry {
public:
	CrossingTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                          CrossingSource &source, string source_schema, CrossingTable described);

	CrossingSource &source;
	string source_schema;
	CrossingTable described;

	const ColumnList &GetColumns() const override {
		return columns;
	}
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	//! Without the source default: what is built from this, such as a local copy, has no source to ask.
	unique_ptr<CreateInfo> GetInfo() const override;

private:
	ColumnList columns;
};

optional_ptr<CrossingTableCatalogEntry> CrossingTableOf(TableCatalogEntry &table, const CrossingIdentity &identity);

TableStorageInfo UniqueConstraintStorageInfo(TableCatalogEntry &table);

//! Makes far's row id a column of each crossing scan that reads it. A scan still reading here is refused unless
//! granted 'select'.
void ResolveRowIds(LogicalOperator &plan, const CrossingIdentity &identity);

} // namespace duckdb
