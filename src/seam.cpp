#include "internal/seam.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

namespace {

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

} // namespace

TableFunction CrossingSeamFunction() {
	TableFunction function(CROSSING_SEAM_FUNCTION, {}, nullptr);
	return function;
}

TableFunction CrossingWriteNodeFunction() {
	TableFunction function(CROSSING_WRITE_NODE_FUNCTION, {}, nullptr);
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

PhysicalOperator &LogicalNearPassThrough::CreatePlan(ClientContext &, PhysicalPlanGenerator &) {
	throw InternalException("crossing: a near node's pass-through outlived the optimizers");
}

string LogicalNearPassThrough::GetName() const {
	return "CROSSING_NEAR_PASS_THROUGH";
}

string LogicalNearPassThrough::GetExtensionName() const {
	return "crossing";
}

void LogicalNearPassThrough::Serialize(Serializer &) const {
	throw NotImplementedException("crossing: a near node is planned, never serialized");
}

unique_ptr<LogicalOperator> MakeNearChild(unique_ptr<LogicalOperator> rows, const FreshTableIndex &fresh) {
	// The near node reads its child by position, but the optimizers that run after this pass reorder by
	// binding: a build/probe swap of a cross product flips its columns. A projection holds the order.
	rows->ResolveOperatorTypes();
	auto bindings = rows->GetColumnBindings();
	vector<unique_ptr<Expression>> columns;
	for (idx_t i = 0; i < bindings.size(); i++) {
		columns.push_back(make_uniq<BoundColumnRefExpression>(rows->types[i], bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(fresh(), std::move(columns));
	projection->children.push_back(std::move(rows));
	auto pass_through = make_uniq<LogicalNearPassThrough>();
	pass_through->children.push_back(std::move(projection));
	return std::move(pass_through);
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
