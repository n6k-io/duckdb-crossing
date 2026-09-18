#include "internal/crossing_shape.hpp"

#include "internal/crossing_write.hpp"
#include "internal/seam_split.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

namespace {

optional_ptr<CrossingTableCatalogEntry> CrossingTableOfWrite(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UPDATE:
		return dynamic_cast<CrossingTableCatalogEntry *>(&op.Cast<LogicalUpdate>().table);
	case LogicalOperatorType::LOGICAL_DELETE:
		return dynamic_cast<CrossingTableCatalogEntry *>(&op.Cast<LogicalDelete>().table);
	case LogicalOperatorType::LOGICAL_INSERT:
		return dynamic_cast<CrossingTableCatalogEntry *>(&op.Cast<LogicalInsert>().table);
	default:
		return nullptr;
	}
}

CrossingVerb VerbOf(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		return CrossingVerb::DELETE_;
	case LogicalOperatorType::LOGICAL_INSERT:
		return CrossingVerb::INSERT;
	default:
		return CrossingVerb::UPDATE;
	}
}

bool ReturnsRows(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		return op.Cast<LogicalDelete>().return_chunk;
	case LogicalOperatorType::LOGICAL_INSERT:
		return op.Cast<LogicalInsert>().return_chunk;
	default:
		return op.Cast<LogicalUpdate>().return_chunk;
	}
}

idx_t TableIndexOf(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		return op.Cast<LogicalDelete>().table_index;
	case LogicalOperatorType::LOGICAL_INSERT:
		return op.Cast<LogicalInsert>().table_index;
	default:
		return op.Cast<LogicalUpdate>().table_index;
	}
}

vector<string> SetColumnsOf(LogicalOperator &op, CrossingTableCatalogEntry &table) {
	vector<string> out;
	if (op.type != LogicalOperatorType::LOGICAL_UPDATE) {
		return out;
	}
	for (auto &column_index : op.Cast<LogicalUpdate>().columns) {
		out.push_back(table.GetColumns().GetColumn(column_index).GetName());
	}
	return out;
}

unique_ptr<LogicalOperator> WholeWrite(idx_t table_index, CrossingTableCatalogEntry &table, CrossingVerb verb,
                                       CrossingSeam seam, shared_ptr<CrossingFragment> fragment) {
	auto bind_data = make_uniq<CrossingWriteBindData>();
	bind_data->table = &table;
	bind_data->verb = verb;
	bind_data->seam = std::move(seam);
	bind_data->fragment = std::move(fragment);
	auto get = make_uniq<LogicalGet>(table_index, CrossingWriteFunction(), std::move(bind_data),
	                                 vector<LogicalType> {LogicalType::BIGINT}, vector<string> {"Count"});
	get->SetColumnIds({ColumnIndex(0)});
	return std::move(get);
}

unique_ptr<LogicalOperator> SeamInsert(ClientContext &context, idx_t table_index, CrossingTableCatalogEntry &table,
                                       CrossingVerb verb, CrossingSeam seam, shared_ptr<CrossingFragment> fragment,
                                       bool returning, vector<idx_t> key_positions, SeamSplit split) {
	CreateTableInfo info(table.schema, table.name);
	if (returning) {
		info.columns = table.GetColumns().Copy();
	} else {
		for (idx_t c = 0; c < split.boundary_types.size(); c++) {
			info.columns.AddColumn(ColumnDefinition("c" + to_string(c), split.boundary_types[c]));
		}
	}
	auto entry = make_shared_ptr<CrossingSeamEntry>(table.catalog, table.schema, info, table, verb, std::move(seam),
	                                                std::move(fragment), std::move(split.obstacle));
	entry->key_positions = std::move(key_positions);
	CrossingSeamEntries::Get(context)->Hold(entry);

	auto insert = make_uniq<LogicalInsert>(*entry, table_index);
	insert->return_chunk = returning;
	insert->expected_types = std::move(split.boundary_types);
	insert->children.push_back(std::move(split.remainder));
	return std::move(insert);
}

bool IsConstantRows(const LogicalExpressionGet &get) {
	for (auto &row : get.expressions) {
		for (auto &expr : row) {
			if (!expr->IsFoldable() || !IsColumnFreeExpression(*expr)) {
				return false;
			}
		}
	}
	return true;
}

void MaterialiseConstantRows(ClientContext &context, unique_ptr<LogicalOperator> &node) {
	for (auto &child : node->children) {
		MaterialiseConstantRows(context, child);
	}
	if (node->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		return;
	}
	auto &get = node->Cast<LogicalExpressionGet>();
	if (!IsConstantRows(get)) {
		return;
	}
	auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), get.expr_types);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), get.expr_types);
	for (auto &row : get.expressions) {
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection->Append(chunk);
			chunk.Reset();
		}
		for (idx_t c = 0; c < row.size(); c++) {
			chunk.SetValue(c, chunk.size(), ExpressionExecutor::EvaluateScalar(context, *row[c]));
		}
		chunk.SetCardinality(chunk.size() + 1);
	}
	if (chunk.size() > 0) {
		collection->Append(chunk);
	}
	node = make_uniq<LogicalColumnDataGet>(get.table_index, get.expr_types, std::move(collection));
}

void ShapeWrite(ClientContext &context, unique_ptr<LogicalOperator> &node, const FreshTableIndex &fresh) {
	auto table = CrossingTableOfWrite(*node);
	if (!table) {
		return;
	}
	auto verb = VerbOf(*node);
	RequireVerb(*table, verb);
	if (verb != CrossingVerb::INSERT) {
		RequireKey(*table, verb == CrossingVerb::UPDATE ? "an update" : "a delete");
	}
	if (node->children.size() != 1) {
		throw NotImplementedException("crossing: %s without a source of rows", CrossingVerbName(verb));
	}

	auto seam = SeamOf(*table, verb, SetColumnsOf(*node, *table));
	auto fragment = PlanWriteFragment(*table, verb, seam);
	auto keys = table->KeyColumnIndexes();
	auto returning = ReturnsRows(*node);
	auto table_index = TableIndexOf(*node);
	MaterialiseConstantRows(context, node->children[0]);
	auto row = SeamRowOf(*node, keys, std::move(node->children[0]), fresh);

	SeamStop stop;
	vector<idx_t> key_positions;
	if (verb != CrossingVerb::INSERT && !table->described.key_unique) {
		stop.node = row.plan.get();
		stop.reason = "the source does not vouch for the key's uniqueness";
		for (idx_t k = 0; k < keys.size(); k++) {
			key_positions.push_back(returning ? keys[k] : k);
		}
	}
	if (returning) {
		stop.node = row.image;
		stop.reason = "RETURNING needs the rows on the target";
	}

	auto split = SplitFeedIntoSeam(std::move(row.plan), *fragment, table->Source(), stop, fresh);
	if (!split.remainder) {
		node = WholeWrite(table_index, *table, verb, std::move(seam), std::move(fragment));
		return;
	}
	node = SeamInsert(context, table_index, *table, verb, std::move(seam), std::move(fragment), returning,
	                  std::move(key_positions), std::move(split));
}

} // namespace

void ShapeWrites(ClientContext &context, unique_ptr<LogicalOperator> &plan, const FreshTableIndex &fresh) {
	for (auto &child : plan->children) {
		ShapeWrites(context, child, fresh);
	}
	ShapeWrite(context, plan, fresh);
}

} // namespace duckdb
