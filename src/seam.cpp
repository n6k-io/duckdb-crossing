#include "internal/seam.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

void SerializeSeam(Serializer &, const optional_ptr<FunctionData>, const BoundTableFunction &) {
}

unique_ptr<FunctionData> DeserializeSeam(Deserializer &, BoundTableFunction &) {
	return nullptr;
}

struct CrossingWriteNodeBindData : public FunctionData {
	CrossingWriteNode node;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<CrossingWriteNodeBindData>();
		copy->node = node;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CrossingWriteNodeBindData>().node;
		return node.verb == other.verb && node.catalog == other.catalog && node.schema == other.schema &&
		       node.table == other.table && node.key_columns == other.key_columns &&
		       node.set_columns == other.set_columns && node.returning_columns == other.returning_columns &&
		       node.returning_types == other.returning_types;
	}
};

void SerializeWriteNode(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const BoundTableFunction &) {
	auto &node = bind_data->Cast<CrossingWriteNodeBindData>().node;
	serializer.WriteProperty(100, "verb", static_cast<uint8_t>(node.verb));
	serializer.WriteProperty(101, "catalog", node.catalog);
	serializer.WriteProperty(102, "schema", node.schema);
	serializer.WriteProperty(103, "table", node.table);
	serializer.WriteProperty(104, "key_columns", node.key_columns);
	serializer.WriteProperty(105, "set_columns", node.set_columns);
	serializer.WriteProperty(106, "returning_columns", node.returning_columns);
	serializer.WriteProperty(107, "returning_types", node.returning_types);
}

unique_ptr<FunctionData> DeserializeWriteNode(Deserializer &deserializer, BoundTableFunction &) {
	auto bind_data = make_uniq<CrossingWriteNodeBindData>();
	auto &node = bind_data->node;
	uint8_t verb;
	deserializer.ReadProperty(100, "verb", verb);
	node.verb = static_cast<CrossingVerb>(verb);
	deserializer.ReadProperty(101, "catalog", node.catalog);
	deserializer.ReadProperty(102, "schema", node.schema);
	deserializer.ReadProperty(103, "table", node.table);
	deserializer.ReadProperty(104, "key_columns", node.key_columns);
	deserializer.ReadProperty(105, "set_columns", node.set_columns);
	deserializer.ReadProperty(106, "returning_columns", node.returning_columns);
	deserializer.ReadProperty(107, "returning_types", node.returning_types);
	return std::move(bind_data);
}

} // namespace

TableFunction CrossingSeamFunction() {
	TableFunction function(CROSSING_SEAM_FUNCTION, {}, nullptr);
	function.serialize = SerializeSeam;
	function.deserialize = DeserializeSeam;
	return function;
}

TableFunction CrossingWriteNodeFunction() {
	TableFunction function(CROSSING_WRITE_NODE_FUNCTION, {}, nullptr);
	function.serialize = SerializeWriteNode;
	function.deserialize = DeserializeWriteNode;
	return function;
}

unique_ptr<LogicalOperator> MakeWriteNode(idx_t table_index, CrossingWriteNode node, unique_ptr<LogicalOperator> feed) {
	auto bind_data = make_uniq<CrossingWriteNodeBindData>();
	bind_data->node = std::move(node);
	auto get = make_uniq<LogicalGet>(TableIndex(table_index), BoundTableFunction(CrossingWriteNodeFunction()),
	                                 std::move(bind_data), vector<LogicalType> {LogicalType::BIGINT},
	                                 vector<Identifier> {"Count"});
	get->SetColumnIds({ColumnIndex(0)});
	if (feed) {
		get->children.push_back(std::move(feed));
	}
	return std::move(get);
}

optional_ptr<const CrossingWriteNode> WriteOf(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (get.function.GetName() != CROSSING_WRITE_NODE_FUNCTION || !get.bind_data) {
		return nullptr;
	}
	return &get.bind_data->Cast<CrossingWriteNodeBindData>().node;
}

optional_ptr<const ColumnDataCollection> SeamRowsOf(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_CHUNK_GET) {
		return nullptr;
	}
	return op.Cast<LogicalColumnDataGet>().collection.get();
}

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types) {
	vector<Identifier> names;
	vector<ColumnIndex> ids;
	for (idx_t i = 0; i < types.size(); i++) {
		names.emplace_back("c" + to_string(i));
		ids.push_back(ColumnIndex(i));
	}
	auto get = make_uniq<LogicalGet>(TableIndex(table_index), BoundTableFunction(CrossingSeamFunction()), nullptr,
	                                 std::move(types), std::move(names));
	get->SetColumnIds(std::move(ids));
	return std::move(get);
}

unique_ptr<LogicalOperator> *FindSeamSlot(unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return nullptr;
	}
	if (plan->type == LogicalOperatorType::LOGICAL_GET &&
	    plan->Cast<LogicalGet>().function.GetName() == CROSSING_SEAM_FUNCTION) {
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
