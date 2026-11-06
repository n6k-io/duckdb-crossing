#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

constexpr const char *CROSSING_SEAM_FUNCTION = "crossing_seam";
constexpr const char *CROSSING_WRITE_NODE_FUNCTION = "crossing_write_node";
constexpr const char *CROSSING_ROW_ID_COLUMN = "rowid";

//! `types` is key_columns then set_columns.
struct CrossingSeam {
	vector<string> key_columns;
	vector<string> set_columns;
	vector<LogicalType> types;
	vector<string> returning_columns;
	vector<LogicalType> returning_types;
};

//! The top of a write's slice: what to write. Its child is what it writes from.
struct CrossingWriteNode {
	CrossingVerb verb = CrossingVerb::INSERT;
	string catalog;
	string schema;
	string table;
	//! `rowid` when the write's rows are addressed through what its child reads; otherwise empty.
	vector<string> key_columns;
	vector<string> set_columns;
	vector<string> returning_columns;
	vector<LogicalType> returning_types;
};

//! The write `op` is the node of. Null for any other operator.
optional_ptr<const CrossingWriteNode> WriteOf(const LogicalOperator &op);

TableFunction CrossingSeamFunction();
TableFunction CrossingWriteNodeFunction();

unique_ptr<LogicalOperator> MakeSeamNode(idx_t table_index, vector<LogicalType> types);

//! `feed` may be null: a write that takes nothing.
unique_ptr<LogicalOperator> MakeWriteNode(idx_t table_index, CrossingWriteNode node, unique_ptr<LogicalOperator> feed);

//! Holds a near node's Get off its rows through the optimizers: an empty child would otherwise take the
//! Get with it (empty_result_pullup.cpp), and the call still has to be made. Gone by the narrow pass.
class LogicalNearPassThrough : public LogicalExtensionOperator {
public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	string GetName() const override;
	string GetExtensionName() const override;
	bool SupportSerialization() const override {
		return false;
	}
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

//! What a near node's Get takes as its child: `rows`, in seam order, behind the pass-through.
unique_ptr<LogicalOperator> MakeNearChild(unique_ptr<LogicalOperator> rows, const FreshTableIndex &fresh);

unique_ptr<LogicalOperator> *FindSeamSlot(unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
