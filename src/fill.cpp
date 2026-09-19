#include "internal/fill.hpp"

#include "internal/carrier.hpp"
#include "internal/filter_halves.hpp"
#include "internal/fold.hpp"
#include "internal/labelling.hpp"
#include "internal/rules.hpp"
#include "internal/seam.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

namespace {

string NameOfColumn(LogicalOperator &below, idx_t position) {
	switch (below.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return below.expressions[position]->GetName();
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = below.Cast<LogicalGet>();
		auto &ids = get.GetColumnIds();
		if (position < ids.size() && !ids[position].IsVirtualColumn() &&
		    ids[position].GetPrimaryIndex() < get.names.size()) {
			return get.names[ids[position].GetPrimaryIndex()];
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
	return make_uniq<BoundColumnRefExpression>(NameOfColumn(below, position), below.types[position],
	                                           below.GetColumnBindings()[position]);
}

unique_ptr<Expression> Named(unique_ptr<Expression> expr, LogicalOperator &feed) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF && expr->alias.empty()) {
		auto &ref = expr->Cast<BoundColumnRefExpression>();
		auto bindings = feed.GetColumnBindings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == ref.binding) {
				ref.alias = NameOfColumn(feed, i);
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

vector<unique_ptr<Expression>> KeysThenSetFromImage(LogicalOperator &image, const vector<column_t> &key_columns,
                                                    const vector<PhysicalIndex> &set_columns) {
	vector<unique_ptr<Expression>> row;
	for (auto key : key_columns) {
		row.push_back(RefTo(image, key));
	}
	for (auto &set : set_columns) {
		row.push_back(RefTo(image, set.index));
	}
	return row;
}

CrossingFeed InsertFeed(LogicalInsert &insert, unique_ptr<LogicalOperator> feed, const FreshTableIndex &fresh) {
	feed->ResolveOperatorTypes();
	vector<unique_ptr<Expression>> row;
	if (insert.column_index_map.empty()) {
		for (idx_t c = 0; c < feed->types.size(); c++) {
			row.push_back(RefTo(*feed, c));
		}
	} else {
		for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
			auto from = insert.column_index_map[PhysicalIndex(c)];
			if (from == DConstants::INVALID_INDEX) {
				row.push_back(insert.bound_defaults[c]->Copy());
			} else {
				row.push_back(RefTo(*feed, from));
			}
		}
	}
	CrossingFeed result;
	result.plan = ProjectionOver(std::move(row), std::move(feed), fresh);
	if (insert.return_chunk) {
		result.image = result.plan.get();
	}
	return result;
}

CrossingFeed UpdateFeed(LogicalUpdate &update, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                        const FreshTableIndex &fresh) {
	feed->ResolveOperatorTypes();
	CrossingFeed result;
	if (!update.return_chunk) {
		vector<unique_ptr<Expression>> row;
		auto first_key = feed->types.size() - key_columns.size();
		for (idx_t k = 0; k < key_columns.size(); k++) {
			row.push_back(RefTo(*feed, first_key + k));
		}
		for (idx_t i = 0; i < update.expressions.size(); i++) {
			row.push_back(Named(SetValueOf(update, i), *feed));
		}
		result.plan = ProjectionOver(std::move(row), std::move(feed), fresh);
		return result;
	}

	vector<unique_ptr<Expression>> image;
	for (idx_t c = 0; c < update.bound_defaults.size(); c++) {
		idx_t set = update.columns.size();
		for (idx_t i = 0; i < update.columns.size(); i++) {
			if (update.columns[i].index == c) {
				set = i;
				break;
			}
		}
		if (set == update.columns.size()) {
			throw BinderException("crossing: RETURNING needs a value for every column of the row");
		}
		image.push_back(Named(SetValueOf(update, set), *feed));
	}
	auto image_plan = ProjectionOver(std::move(image), std::move(feed), fresh);
	result.image = image_plan.get();
	auto seam_row = KeysThenSetFromImage(*image_plan, key_columns, update.columns);
	result.plan = ProjectionOver(std::move(seam_row), std::move(image_plan), fresh);
	return result;
}

CrossingFeed DeleteFeed(LogicalDelete &del, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                        const FreshTableIndex &fresh) {
	feed->ResolveOperatorTypes();
	CrossingFeed result;
	auto key_count = key_columns.size();
	if (!del.return_chunk) {
		vector<unique_ptr<Expression>> row;
		for (idx_t k = 0; k < key_count; k++) {
			row.push_back(Named(del.expressions[k]->Copy(), *feed));
		}
		result.plan = ProjectionOver(std::move(row), std::move(feed), fresh);
		return result;
	}

	vector<unique_ptr<Expression>> image;
	for (idx_t i = key_count; i < del.expressions.size(); i++) {
		image.push_back(Named(del.expressions[i]->Copy(), *feed));
	}
	auto image_plan = ProjectionOver(std::move(image), std::move(feed), fresh);
	result.image = image_plan.get();
	auto seam_row = KeysThenSetFromImage(*image_plan, key_columns, {});
	result.plan = ProjectionOver(std::move(seam_row), std::move(image_plan), fresh);
	return result;
}

string WhyItStays(LogicalOperator &node, CrossingSource &source) {
	auto &identity = source.Identity();
	auto name = StringUtil::Lower(LogicalOperatorToString(node.type));
	if (CrossingReadFragmentOf(node, identity)) {
		if (CrossingSourceOf(node, identity).get() == &source) {
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

} // namespace

CrossingFeed FeedOf(LogicalOperator &write, const vector<column_t> &key_columns, unique_ptr<LogicalOperator> feed,
                    const FreshTableIndex &fresh) {
	switch (write.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
		return InsertFeed(write.Cast<LogicalInsert>(), std::move(feed), fresh);
	case LogicalOperatorType::LOGICAL_UPDATE:
		return UpdateFeed(write.Cast<LogicalUpdate>(), key_columns, std::move(feed), fresh);
	case LogicalOperatorType::LOGICAL_DELETE:
		return DeleteFeed(write.Cast<LogicalDelete>(), key_columns, std::move(feed), fresh);
	default:
		throw InternalException("crossing: a %s has no feed", LogicalOperatorToString(write.type));
	}
}

SeamFill FillSeamFromFeed(unique_ptr<LogicalOperator> feed, CrossingFragment &fragment, CrossingSource &source,
                          const SeamObstacle &stop, const FreshTableIndex &fresh) {
	auto &identity = source.Identity();
	if (!fragment.HasSeam()) {
		throw InternalException("crossing: the fragment has no seam to fill");
	}
	fragment.RemapTableIndices(fresh);
	auto slot = fragment.SeamSlot();
	auto seam_index = (*slot)->Cast<LogicalGet>().table_index;

	SplitFiltersAgainst(feed, source);
	auto labels = LabelSubtreesUnder(*feed, source);

	SeamFill fill;
	unique_ptr<LogicalOperator> *cursor = &feed;
	bool whole = false;
	while (true) {
		auto &node = **cursor;
		if (stop.node.get() == &node) {
			fill.obstacle = stop.obstacle;
			break;
		}
		auto label = labels.find(&node);
		if (!stop.node && label != labels.end() && (label->second.runs_anywhere || label->second.source == &source) &&
		    !SubtreeHoldsFrozenScan(node, identity)) {
			whole = true;
			break;
		}
		fill.obstacle = WhyItStays(node, source);
		if (!fill.obstacle.empty()) {
			break;
		}
		cursor = &node.children[0];
	}

	if (whole) {
		UnfoldScansIntoPlace(*cursor, identity);
		fill.seam_types = fragment.seam_types;
	} else {
		auto &boundary = *cursor;
		boundary->ResolveOperatorTypes();
		fill.seam_types = boundary->types;
		if (cursor == &feed) {
			if (fill.seam_types != fragment.seam_types) {
				throw InternalException("crossing: the seam row does not fit the seam");
			}
			fill.remainder = std::move(feed);
			RejoinAdjacentFiltersEverywhere(fill.remainder, identity);
			return fill;
		}
		auto bindings = boundary->GetColumnBindings();
		auto seam2_index = fresh();
		fill.remainder = std::move(boundary);
		boundary = MakeSeamNode(seam2_index, fill.seam_types);
		ColumnBindingReplacer replacer;
		for (idx_t i = 0; i < bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(bindings[i], ColumnBinding(seam2_index, i));
		}
		replacer.VisitOperator(*feed);
		RejoinAdjacentFiltersEverywhere(fill.remainder, identity);
	}

	AdoptSeamIndex(*feed, seam_index);
	*slot = std::move(feed);
	fragment.seam_types = fill.seam_types;
	fragment.ResolveTypesAndText();
	fragment.VerifyInvariants();
	RejoinAdjacentFiltersEverywhere(fragment.plan, identity);
	fragment.ResolveTypesAndText();
	return fill;
}

} // namespace duckdb
