#include "internal/fill.hpp"

#include "internal/carrier.hpp"
#include "internal/filter_halves.hpp"
#include "internal/fold.hpp"
#include "internal/labelling.hpp"
#include "internal/rules.hpp"
#include "internal/seam.hpp"
#include "internal/source_evaluation.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

namespace {

constexpr const char *SOURCE_DEFAULT_FUNCTION = "set_by_source";

void ThrowSourceDefault(DataChunk &, ExpressionState &, Vector &) {
	throw InvalidInputException("crossing: only the source knows this column's default, so it cannot be computed "
	                            "here. Leave the column out of an INSERT; DEFAULT itself is not supported");
}

string NameOfColumn(LogicalOperator &below, idx_t position) {
	switch (below.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return below.expressions[position]->GetName().GetIdentifierName();
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = below.Cast<LogicalGet>();
		auto &ids = get.GetColumnIds();
		if (position < ids.size() && !ids[position].IsVirtualColumn() &&
		    ids[position].GetPrimaryIndex() < get.names.size()) {
			return get.names[ids[position].GetPrimaryIndex()].GetIdentifierName();
		}
		return "c" + to_string(position);
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return NameOfColumn(*below.children[0], position);
	default:
		return "c" + to_string(position);
	}
}

unique_ptr<Expression> RefTo(LogicalOperator &below, idx_t position) {
	return make_uniq<BoundColumnRefExpression>(Identifier(NameOfColumn(below, position)), below.types[position],
	                                           below.GetColumnBindings()[position]);
}

unique_ptr<Expression> Named(unique_ptr<Expression> expr, LogicalOperator &feed) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF && !expr->HasAlias()) {
		auto &ref = expr->Cast<BoundColumnRefExpression>();
		auto bindings = feed.GetColumnBindings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == ref.Binding()) {
				ref.SetAlias(Identifier(NameOfColumn(feed, i)));
				break;
			}
		}
	}
	return expr;
}

unique_ptr<LogicalOperator> ProjectionOver(vector<unique_ptr<Expression>> expressions,
                                           unique_ptr<LogicalOperator> child, const FreshTableIndex &fresh) {
	auto projection = make_uniq<LogicalProjection>(fresh(), std::move(expressions));
	projection->children.push_back(std::move(child));
	projection->ResolveOperatorTypes();
	return std::move(projection);
}

unique_ptr<Expression> SetValueOf(LogicalUpdate &update, idx_t i) {
	if (update.expressions[i]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
		return update.bound_defaults[update.columns[i].index]->Copy();
	}
	return update.expressions[i]->Copy();
}

CrossingFeed InsertFeed(unique_ptr<LogicalOperator> feed, const FreshTableIndex &fresh) {
	feed->ResolveOperatorTypes();
	vector<unique_ptr<Expression>> row;
	for (idx_t c = 0; c < feed->types.size(); c++) {
		row.push_back(RefTo(*feed, c));
	}
	CrossingFeed result;
	result.plan = ProjectionOver(std::move(row), std::move(feed), fresh);
	return result;
}

CrossingFeed UpdateFeed(LogicalUpdate &update, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                        const FreshTableIndex &fresh) {
	feed->ResolveOperatorTypes();
	vector<unique_ptr<Expression>> row;
	auto first_key = feed->types.size() - key_columns.size();
	for (idx_t k = 0; k < key_columns.size(); k++) {
		row.push_back(RefTo(*feed, first_key + k));
	}
	for (idx_t i = 0; i < update.expressions.size(); i++) {
		row.push_back(Named(SetValueOf(update, i), *feed));
	}
	CrossingFeed result;
	result.plan = ProjectionOver(std::move(row), std::move(feed), fresh);
	return result;
}

CrossingFeed DeleteFeed(LogicalDelete &del, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                        const FreshTableIndex &fresh) {
	feed->ResolveOperatorTypes();
	vector<unique_ptr<Expression>> row;
	for (idx_t k = 0; k < key_columns.size(); k++) {
		row.push_back(Named(del.expressions[k]->Copy(), *feed));
	}
	CrossingFeed result;
	result.plan = ProjectionOver(std::move(row), std::move(feed), fresh);
	return result;
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

bool IsConsistentRows(const LogicalExpressionGet &get) {
	for (auto &row : get.expressions) {
		for (auto &expr : row) {
			if (!expr->IsConsistent()) {
				return false;
			}
		}
	}
	return true;
}

string WhyItStays(LogicalOperator &node, CrossingSource &source) {
	auto &identity = source.Identity();
	auto name = StringUtil::Lower(LogicalOperatorToString(node.type));
	if (CrossingReadFragmentOf(node, identity)) {
		if (CrossingSourceOf(node, identity)->Group() == source.Group()) {
			return "a frozen scan stays on the target";
		}
		return "a read of another source stays on the target";
	}
	if (node.children.size() != 1) {
		return name + " stays on the target";
	}
	auto verdict = NodeVerdict(node, source);
	if (!verdict.ok) {
		return name + " is not something the source can compute: " + verdict.reason;
	}
	return string();
}

string WhyNotWhole(LogicalOperator &feed, const SubtreeLabels &labels, CrossingSource &source) {
	auto label = labels.find(&feed);
	if (label != labels.end() && !label->second.source) {
		return "the feed reads nothing of the source";
	}
	optional_ptr<LogicalOperator> node = &feed;
	while (true) {
		auto reason = WhyItStays(*node, source);
		if (!reason.empty()) {
			return reason;
		}
		node = node->children[0].get();
	}
}

void RequireSeamTakesRow(LogicalOperator &feed, const CrossingFragment &fragment) {
	if (feed.types != fragment.seam_types) {
		throw InternalException("crossing: the seam row does not fit the seam");
	}
}

} // namespace

CrossingFeed FeedOf(LogicalOperator &write, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                    const FreshTableIndex &fresh) {
	switch (write.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
		return InsertFeed(std::move(feed), fresh);
	case LogicalOperatorType::LOGICAL_UPDATE:
		return UpdateFeed(write.Cast<LogicalUpdate>(), key_columns, std::move(feed), fresh);
	case LogicalOperatorType::LOGICAL_DELETE:
		return DeleteFeed(write.Cast<LogicalDelete>(), key_columns, std::move(feed), fresh);
	default:
		throw InternalException("crossing: a %s has no feed", LogicalOperatorToString(write.type));
	}
}

SeamFill FillSeamFromFeed(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment, CrossingSource &source,
                          const SeamObstacle &stop, const FreshTableIndex &fresh, bool single_joins_cross) {
	auto &identity = source.Identity();
	if (!fragment.HasSeam()) {
		throw InternalException("crossing: the fragment has no seam to fill");
	}
	fragment.RemapTableIndices(fresh);
	auto slot = fragment.SeamSlot();
	auto seam_index = (*slot)->Cast<LogicalGet>().table_index;

	auto labels = LabelSubtreesUnder(*feed, source, single_joins_cross);

	SeamFill fill;
	fill.seam_types = fragment.seam_types;
	auto label = labels.find(feed.get());
	// A label without a source means the rows are already on the target, so moving work onto them saves nothing.
	bool whole = !stop.stays && label != labels.end() && label->second.source &&
	             label->second.source->Group() == source.Group() && !SubtreeHoldsFrozenScan(*feed, identity);
	if (!whole) {
		fill.obstacle = stop.stays ? stop.obstacle : WhyNotWhole(*feed, labels, source);
		feed->ResolveOperatorTypes();
		RequireSeamTakesRow(*feed, fragment);
		fill.remainder = std::move(feed);
		RejoinAdjacentFiltersEverywhere(fill.remainder, identity);
		return fill;
	}

	auto near_child = UnfoldScansIntoPlace(feed, identity);
	AdoptSeamIndex(*feed, seam_index);
	*slot = std::move(feed);
	if (near_child) {
		// The write's own seam is filled by this work, and the work's seam is what near sends.
		fill.remainder = std::move(near_child->children[0]);
		fill.remainder->ResolveOperatorTypes();
		fragment.seam_types = fill.remainder->types;
	}
	fragment.ResolveTypesAndText();
	fragment.VerifyInvariants();
	RejoinAdjacentFiltersEverywhere(fragment.plan, identity);
	fragment.ResolveTypesAndText();
	return fill;
}

unique_ptr<ParsedExpression> SourceDefaultExpression() {
	return make_uniq<FunctionExpression>(Identifier(SOURCE_DEFAULT_FUNCTION), vector<unique_ptr<ParsedExpression>>());
}

void RegisterSourceDefaultFunction(DatabaseInstance &db) {
	ScalarFunction function(SOURCE_DEFAULT_FUNCTION, {}, LogicalType::SQLNULL, ThrowSourceDefault);
	function.SetVolatile();
	function.SetFallible();
	CreateScalarFunctionInfo info(function);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	Catalog::GetSystemCatalog(db).CreateFunction(CatalogTransaction::GetSystemTransaction(db), info);
}

bool IsSourceDefault(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &function = expr.Cast<BoundFunctionExpression>();
	if (BoundCastExpression::IsCast(expr)) {
		return IsSourceDefault(BoundCastExpression::Child(function));
	}
	return function.Function().GetName() == SOURCE_DEFAULT_FUNCTION;
}

void MaterialiseConstantRows(ClientContext &context, unique_ptr<LogicalOperator> &node, bool only_consistent) {
	for (auto &child : node->children) {
		MaterialiseConstantRows(context, child, only_consistent);
	}
	if (node->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
		return;
	}
	auto &get = node->Cast<LogicalExpressionGet>();
	if (!IsConstantRows(get) || (only_consistent && !IsConsistentRows(get))) {
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
			chunk.data[c].SetValue(chunk.size(), ExpressionExecutor::EvaluateScalar(context, *row[c]));
		}
		chunk.SetChildCardinality(chunk.size() + 1);
	}
	if (chunk.size() > 0) {
		collection->Append(chunk);
	}
	node = make_uniq<LogicalColumnDataGet>(get.table_index, get.expr_types, std::move(collection));
}

} // namespace duckdb
