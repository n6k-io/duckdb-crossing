#include "internal/crossing_table_entry.hpp"
#include "internal/seam.hpp"

#include "internal/crossing_read.hpp"
#include "internal/crossing_write.hpp"

#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {} // namespace

CrossingTableCatalogEntry::CrossingTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                     CreateTableInfo &info, CrossingSource &source_p,
                                                     string source_schema_p, CrossingTable described_p)
    : TableCatalogEntry(catalog, schema, info), source(source_p), source_schema(std::move(source_schema_p)),
      described(std::move(described_p)), columns(info.columns.Copy()) {
}

unique_ptr<CreateInfo> CrossingTableCatalogEntry::GetInfo() const {
	auto info = TableCatalogEntry::GetInfo();
	ColumnList plain;
	for (auto &column : GetColumns().Logical()) {
		plain.AddColumn(ColumnDefinition(column.Name(), column.Type()));
	}
	info->Cast<CreateTableInfo>().columns = std::move(plain);
	return info;
}

unique_ptr<BaseStatistics> CrossingTableCatalogEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

TableFunction CrossingTableCatalogEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &bind_data) {
	// An update or delete binds this scan too, so 'select' is required only once the pass sees the
	// scan read (ResolveKeyAliases).
	if (!described.Allows(CrossingVerb::SELECT) && !described.Allows(CrossingVerb::UPDATE) &&
	    !described.Allows(CrossingVerb::DELETE_)) {
		throw PermissionException("crossing: '%s' does not have 'select' permission", name);
	}
	bind_data = MakeReadBindData(*this);
	return CrossingReadFunction();
}

TableStorageInfo CrossingTableCatalogEntry::GetStorageInfo(ClientContext &) {
	return UniqueConstraintStorageInfo(*this);
}

optional_ptr<CrossingTableCatalogEntry> CrossingTableOf(TableCatalogEntry &table, const CrossingIdentity &identity) {
	auto entry = dynamic_cast<CrossingTableCatalogEntry *>(&table);
	if (!entry || &entry->source.Identity() != &identity) {
		return nullptr;
	}
	return entry;
}

TableStorageInfo UniqueConstraintStorageInfo(TableCatalogEntry &table) {
	TableStorageInfo info;
	// Only the unique constraints the source declared, which it enforces. The key is how crossing
	// addresses rows, not a uniqueness anyone keeps, so ON CONFLICT must not be allowed to match on it.
	auto &columns = table.GetColumns();
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		IndexInfo index;
		index.is_unique = true;
		index.is_primary = unique.IsPrimaryKey();
		index.is_foreign = false;
		if (unique.HasIndex()) {
			index.column_set.insert(columns.GetColumn(unique.GetIndex()).Physical().index);
		} else {
			for (auto &column_name : unique.GetColumnNames()) {
				index.column_set.insert(columns.GetColumn(column_name).Physical().index);
			}
		}
		info.index_info.push_back(std::move(index));
	}
	return info;
}

void ResolveRowIds(LogicalOperator &plan, const CrossingIdentity &identity) {
	for (auto &child : plan.children) {
		ResolveRowIds(*child, identity);
	}
	if (plan.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = plan.Cast<LogicalGet>();
	auto data = CrossingBindDataOf(get, identity);
	if (!data || data->verb != CrossingVerb::SELECT) {
		return;
	}
	bool reads = false;
	for (auto &column_index : get.GetColumnIds()) {
		reads |= column_index.IsVirtualColumn() || column_index.GetPrimaryIndex() < get.returned_types.size();
	}
	if (reads) {
		RequireVerb(data->table, CrossingVerb::SELECT);
	}
	for (auto &column_index : get.GetMutableColumnIds()) {
		if (!column_index.IsVirtualColumn()) {
			continue;
		}
		if (column_index.GetPrimaryIndex() != COLUMN_IDENTIFIER_ROW_ID) {
			throw NotImplementedException("crossing: '%s' cannot read this virtual column", data->SourceTable());
		}
		auto column = data->fragment->IncludeRowId();
		column_index = ColumnIndex(column);
		if (column == get.returned_types.size()) {
			get.returned_types.push_back(LogicalType::ROW_TYPE);
			get.names.emplace_back(CROSSING_ROW_ID_COLUMN);
		}
	}
}

} // namespace duckdb
