#pragma once

#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/table_indices.hpp"

namespace duckdb {

//! Bindings in a shaped write's expressions: a target column by its table position, a source column by its
//! position in the source's output.
static constexpr idx_t SHAPED_TARGET_INDEX = idx_t(1) << 40;
static constexpr idx_t SHAPED_SOURCE_INDEX = (idx_t(1) << 40) + 1;

struct ShapedAction {
	MergeActionCondition when;
	MergeActionType type;
	unique_ptr<Expression> condition;
	vector<string> columns;
	vector<unique_ptr<Expression>> values;
};

//! A write as far's native statement would say it: its target, what it is joined to, on what, and what it does.
struct ShapedWrite {
	bool merge = false;
	unique_ptr<Expression> condition;
	vector<string> set_columns;
	vector<unique_ptr<Expression>> values;
	vector<ShapedAction> actions;
	vector<LogicalType> source_types;
	bool has_source = false;
	bool reads_target = false;
};

struct ShapedWrites : public ClientContextState {
	static constexpr const char *KEY = "crossing_shaped_writes";

	map<const LogicalOperator *, shared_ptr<ShapedWrite>> writes;

	static ShapedWrites &Of(ClientContext &context);
	shared_ptr<ShapedWrite> Take(const LogicalOperator &op);
};

//! Turns each UPDATE, DELETE and MERGE of a source's table whose form is target, joined source,
//! condition into a ShapedWrite, leaving only its source below it to fold. What does not have that form is left
//! as it is.
void ShapeWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh,
                 const CrossingIdentity &identity);

} // namespace duckdb
