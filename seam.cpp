#include "internal/seam.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

void SerializeSeam(Serializer &, const optional_ptr<FunctionData>, const TableFunction &) {
}

unique_ptr<FunctionData> DeserializeSeam(Deserializer &, TableFunction &) {
	return nullptr;
}

} // namespace

TableFunction CrossingSeamFunction() {
	TableFunction function(CROSSING_SEAM_FUNCTION, {}, nullptr);
	function.serialize = SerializeSeam;
	function.deserialize = DeserializeSeam;
	return function;
}

optional_ptr<const ColumnDataCollection> SeamRowsOf(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_CHUNK_GET) {
		return nullptr;
	}
	return op.Cast<LogicalColumnDataGet>().collection.get();
}

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types) {
	vector<string> names;
	vector<ColumnIndex> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		names.push_back("c" + to_string(i));
		ids.push_back(ColumnIndex(i));
	}
	auto get = make_uniq<LogicalGet>(table_index, CrossingSeamFunction(), nullptr, std::move(types), std::move(names));
	get->SetColumnIds(std::move(ids));
	return std::move(get);
}

unique_ptr<LogicalOperator> *FindSeamSlot(unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return nullptr;
	}
	if (plan->type == LogicalOperatorType::LOGICAL_GET &&
	    plan->Cast<LogicalGet>().function.name == CROSSING_SEAM_FUNCTION) {
		return &plan;
	}
	for (auto &child : plan->children) {
		if (auto found = FindSeamSlot(child)) {
			return found;
		}
	}
	return nullptr;
}

} // namespace duckdb
