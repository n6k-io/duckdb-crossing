#include "crossing.hpp"
#include "internal/floor.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

struct CrossingFloorBindData : public FunctionData {
	CrossingFloor floor;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<CrossingFloorBindData>();
		copy->floor = floor;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CrossingFloorBindData>();
		return floor.schema == other.floor.schema && floor.table == other.floor.table &&
		       floor.column_names == other.floor.column_names;
	}
};

void SerializeFloor(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction &) {
	auto &floor = bind_data->Cast<CrossingFloorBindData>().floor;
	serializer.WriteProperty(100, "schema", floor.schema);
	serializer.WriteProperty(101, "table", floor.table);
	serializer.WriteProperty(102, "column_names", floor.column_names);
}

unique_ptr<FunctionData> DeserializeFloor(Deserializer &deserializer, TableFunction &) {
	auto bind_data = make_uniq<CrossingFloorBindData>();
	deserializer.ReadProperty(100, "schema", bind_data->floor.schema);
	deserializer.ReadProperty(101, "table", bind_data->floor.table);
	deserializer.ReadProperty(102, "column_names", bind_data->floor.column_names);
	return std::move(bind_data);
}

} // namespace

TableFunction CrossingFloorFunction() {
	TableFunction function(CROSSING_FLOOR_FUNCTION, {}, nullptr);
	function.serialize = SerializeFloor;
	function.deserialize = DeserializeFloor;
	return function;
}

unique_ptr<LogicalOperator> MakeFloorNode(idx_t table_index, string schema, string table, vector<string> column_names,
                                          vector<LogicalType> column_types) {
	if (column_names.size() != column_types.size()) {
		throw InternalException("crossing: a floor of '%s' names %llu columns and %llu types", table,
		                        column_names.size(), column_types.size());
	}
	auto bind_data = make_uniq<CrossingFloorBindData>();
	bind_data->floor.schema = std::move(schema);
	bind_data->floor.table = std::move(table);
	bind_data->floor.column_names = column_names;
	vector<ColumnIndex> ids;
	for (idx_t i = 0; i < column_names.size(); i++) {
		ids.push_back(ColumnIndex(i));
	}
	auto get = make_uniq<LogicalGet>(table_index, CrossingFloorFunction(), std::move(bind_data),
	                                 std::move(column_types), std::move(column_names));
	get->SetColumnIds(std::move(ids));
	return std::move(get);
}

optional_ptr<const CrossingFloor> FloorOf(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (get.function.name != CROSSING_FLOOR_FUNCTION || !get.bind_data) {
		return nullptr;
	}
	auto bind_data = dynamic_cast<const CrossingFloorBindData *>(get.bind_data.get());
	if (!bind_data) {
		return nullptr;
	}
	return &bind_data->floor;
}

} // namespace duckdb
