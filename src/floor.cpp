#include "crossing.hpp"
#include "internal/floor.hpp"
#include "internal/names.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

struct CrossingFloorBindData : public FunctionData {
	CrossingFloorNode floor;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<CrossingFloorBindData>();
		copy->floor = floor;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CrossingFloorBindData>();
		return floor.catalog == other.floor.catalog && floor.schema == other.floor.schema &&
		       floor.table == other.floor.table && floor.column_names == other.floor.column_names;
	}
};

} // namespace

TableFunction CrossingFloorFunction() {
	TableFunction function(CROSSING_FLOOR_FUNCTION, {}, nullptr);
	return function;
}

unique_ptr<LogicalOperator> MakeFloorNode(idx_t table_index, string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types, string catalog) {
	if (column_names.size() != column_types.size()) {
		throw InternalException("crossing: a floor of '%s' names %llu columns and %llu types", table,
		                        column_names.size(), column_types.size());
	}
	auto bind_data = make_uniq<CrossingFloorBindData>();
	bind_data->floor.catalog = std::move(catalog);
	bind_data->floor.schema = std::move(schema);
	bind_data->floor.table = std::move(table);
	bind_data->floor.column_names = column_names;
	vector<ColumnIndex> ids;
	for (idx_t i = 0; i < column_names.size(); i++) {
		ids.push_back(ColumnIndex(i));
	}
	auto get = make_uniq<LogicalGet>(TableIndex(table_index), BoundTableFunction(CrossingFloorFunction()),
	                                 std::move(bind_data), std::move(column_types), IdentifiersOf(column_names));
	get->SetColumnIds(std::move(ids));
	return std::move(get);
}

unique_ptr<LogicalOperator> MakeFloorNode(string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types, string catalog) {
	return MakeFloorNode(0, std::move(schema), std::move(table), std::move(column_names), std::move(column_types),
	                     std::move(catalog));
}

optional_ptr<const CrossingFloorNode> FloorOf(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (get.function.GetName() != CROSSING_FLOOR_FUNCTION || !get.bind_data) {
		return nullptr;
	}
	auto bind_data = dynamic_cast<const CrossingFloorBindData *>(get.bind_data.get());
	if (!bind_data) {
		return nullptr;
	}
	return &bind_data->floor;
}

namespace {

void CollectFloors(const LogicalOperator &op, vector<CrossingTableUse> &out) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		CrossingTableUse use;
		bool named = false;
		if (auto floor = FloorOf(get)) {
			use.catalog = floor->catalog;
			use.schema = floor->schema;
			use.table = floor->table;
			named = true;
		} else if (auto table = get.GetTable()) {
			use.schema = table->schema.name.GetIdentifierName();
			use.table = table->name.GetIdentifierName();
			named = true;
		}
		if (named) {
			for (auto &column_index : get.GetColumnIds()) {
				if (column_index.IsVirtualColumn()) {
					continue;
				}
				use.columns.push_back(column_index.GetPrimaryIndex());
			}
			out.push_back(std::move(use));
		}
	}
	for (auto &child : op.children) {
		CollectFloors(*child, out);
	}
}

} // namespace

vector<CrossingTableUse> CrossingTablesOf(const LogicalOperator &plan) {
	vector<CrossingTableUse> out;
	CollectFloors(plan, out);
	return out;
}

} // namespace duckdb
