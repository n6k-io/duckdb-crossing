#include "internal/crossing_table_entry.hpp"

#include "internal/crossing_read.hpp"

#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

namespace {

constexpr column_t KEY_ALIAS_BASE = UINT64_C(9223372036854775808) + 1;

} // namespace

CrossingTableCatalogEntry::CrossingTableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                     CreateTableInfo &info, CrossingSource &source_p,
                                                     string source_schema_p, CrossingTable described_p)
    : TableCatalogEntry(catalog, schema, info), source(source_p), source_schema(std::move(source_schema_p)),
      described(std::move(described_p)) {
}

vector<column_t> CrossingTableCatalogEntry::KeyColumnIndexes() const {
	vector<column_t> out;
	for (auto &key : described.key) {
		out.push_back(GetColumns().GetColumn(key).Logical().index);
	}
	return out;
}

optional_idx CrossingTableCatalogEntry::KeyColumnOfAlias(column_t virtual_id) const {
	if (virtual_id < KEY_ALIAS_BASE) {
		return optional_idx();
	}
	auto key = KeyColumnIndexes();
	auto k = virtual_id - KEY_ALIAS_BASE;
	if (k >= key.size()) {
		return optional_idx();
	}
	return key[k];
}

virtual_column_map_t CrossingTableCatalogEntry::GetVirtualColumns() const {
	auto inherited = TableCatalogEntry::GetVirtualColumns();
	auto key = KeyColumnIndexes();
	for (idx_t k = 0; k < key.size(); k++) {
		auto &column = GetColumns().GetColumn(LogicalIndex(key[k]));
		inherited.insert(make_pair(KEY_ALIAS_BASE + k, TableColumn(column.Name(), column.Type())));
	}
	return inherited;
}

vector<column_t> CrossingTableCatalogEntry::GetRowIdColumns() const {
	vector<column_t> out;
	for (idx_t k = 0; k < described.key.size(); k++) {
		out.push_back(KEY_ALIAS_BASE + k);
	}
	return out;
}

unique_ptr<BaseStatistics> CrossingTableCatalogEntry::GetStatistics(ClientContext &, column_t) {
	return nullptr;
}

TableFunction CrossingTableCatalogEntry::GetScanFunction(ClientContext &, unique_ptr<FunctionData> &bind_data) {
	if (!described.Allows(CrossingVerb::SELECT)) {
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

void ResolveKeyAliases(LogicalOperator &plan, const CrossingIdentity &identity) {
	for (auto &child : plan.children) {
		ResolveKeyAliases(*child, identity);
	}
	if (plan.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = plan.Cast<LogicalGet>();
	auto data = CrossingBindDataOf(get, identity);
	if (!data || data->verb != CrossingVerb::SELECT) {
		return;
	}
	for (auto &column_index : get.GetMutableColumnIds()) {
		if (!column_index.IsVirtualColumn()) {
			continue;
		}
		auto key = data->table.KeyColumnOfAlias(column_index.GetPrimaryIndex());
		if (key.IsValid()) {
			column_index = ColumnIndex(key.GetIndex());
		} else if (data->fragment) {
			data->fragment->frozen = true;
		}
	}
}

} // namespace duckdb
