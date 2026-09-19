#include "crossing.hpp"
#include "internal/floor.hpp"
#include "internal/seam.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

void RegisterCrossingPlanFunctions(DatabaseInstance &db) {
	auto &system = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	for (auto &function : {CrossingFloorFunction(), CrossingSeamFunction()}) {
		CreateTableFunctionInfo info(function);
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		system.CreateFunction(transaction, info);
	}
}

namespace {

void CollectFloors(const LogicalOperator &op, vector<CrossingTableUse> &out) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		CrossingTableUse use;
		bool named = false;
		if (auto floor = FloorOf(get)) {
			use.schema = floor->schema;
			use.table = floor->table;
			named = true;
		} else if (auto table = get.GetTable()) {
			use.schema = table->schema.name;
			use.table = table->name;
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

string SerializeCrossingPlan(const LogicalOperator &plan) {
	MemoryStream stream;
	BinarySerializer::Serialize(plan, stream);
	return string(const_char_ptr_cast(stream.GetData()), stream.GetPosition());
}

unique_ptr<LogicalOperator> DeserializeCrossingPlan(ClientContext &context, const string &bytes) {
	MemoryStream stream(bytes.size());
	stream.WriteData(const_data_ptr_cast(bytes.data()), bytes.size());
	stream.Rewind();
	bound_parameter_map_t parameters;
	auto plan = BinaryDeserializer::Deserialize<LogicalOperator>(stream, context, parameters);
	plan->ResolveOperatorTypes();
	return plan;
}

} // namespace duckdb
