#include "internal/crossing_pass.hpp"

#include "internal/crossing_fill.hpp"
#include "internal/crossing_read.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/crossing_write.hpp"
#include "internal/fill.hpp"
#include "internal/pass.hpp"
#include "internal/shaped_write.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

namespace {

const CrossingIdentity &IdentityOf(OptimizerExtensionInput &input) {
	if (!input.info) {
		throw InternalException("crossing: the pass was registered without its identity");
	}
	return static_cast<CrossingPassInfo &>(*input.info).identity;
}

void SwapCrossingGetsForTheirNodes(ClientContext &context, unique_ptr<LogicalOperator> &node,
                                   const CrossingIdentity &identity) {
	for (auto &child : node->children) {
		SwapCrossingGetsForTheirNodes(context, child, identity);
	}
	if (node->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = node->Cast<LogicalGet>();
	auto data = CrossingBindDataOf(get, identity);
	if (!data) {
		return;
	}
	if (data->verb == CrossingVerb::SELECT) {
		if (get.table_filters.HasFilters() || !get.projection_ids.empty() || !get.projected_input.empty()) {
			throw InternalException("crossing: the scan of '%s' was optimized in a way the read cannot carry",
			                        data->SourceTable());
		}
		get.ResolveOperatorTypes();
		auto cardinality = get.EstimateCardinality(context);
		auto bind_data = unique_ptr_cast<FunctionData, CrossingBindData>(std::move(get.bind_data));
		auto read =
		    make_uniq<LogicalCrossingRead>(get.table_index, get.GetColumnIds(), get.types, std::move(bind_data));
		read->SetEstimatedCardinality(cardinality);
		if (!get.children.empty()) {
			read->children.push_back(std::move(get.children[0]->children[0]));
		}
		node = std::move(read);
		return;
	}
	get.ResolveOperatorTypes();
	vector<optional_idx> emit;
	auto add = [&emit](const ColumnIndex &id) {
		emit.push_back(id.IsVirtualColumn() ? optional_idx() : optional_idx(id.GetPrimaryIndex()));
	};
	auto &ids = get.GetColumnIds();
	if (get.projection_ids.empty()) {
		for (auto &id : ids) {
			add(id);
		}
	} else {
		for (auto &position : get.projection_ids) {
			add(ids[position.GetIndex()]);
		}
	}
	auto bind_data = unique_ptr_cast<FunctionData, CrossingBindData>(std::move(get.bind_data));
	auto near_node = make_uniq<LogicalCrossingNear>(get.table_index, get.GetColumnBindings(), get.types,
	                                                std::move(emit), std::move(bind_data));
	near_node->SetEstimatedCardinality(1);
	if (!get.children.empty()) {
		near_node->children.push_back(std::move(get.children[0]->children[0]));
	}
	node = std::move(near_node);
}

} // namespace

void CrossingFoldPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &identity = IdentityOf(input);
	auto &binder = input.optimizer.binder;
	FreshTableIndex fresh = [&binder]() {
		return binder.GenerateTableIndex();
	};
	// A VALUES list crosses only as rows: a source is never asked about the expressions it holds.
	if (PlanHoldsCrossing(*plan, identity)) {
		MaterialiseConstantRows(input.context, plan);
	}
	RefuseWritesInCtes(*plan, identity);
	LeaveInsertDefaultsToSource(*plan, identity);
	NarrowSetsToWhatChanges(*plan, identity);
	ShapeWrites(input.context, plan, fresh, identity);
	FillBareWrites(input.context, plan, fresh, identity);
	ResolveRowIds(*plan, identity);
	FoldCrossableWorkIntoFragments(plan, fresh, identity,
	                               Settings::Get<ScalarSubqueryErrorOnMultipleRowsSetting>(input.context));
	FillWrites(input.context, plan, fresh, identity);
}

void CrossingNarrowPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &identity = IdentityOf(input);
	NarrowScansToRequestedColumns(plan, identity);
	SwapCrossingGetsForTheirNodes(input.context, plan, identity);
}

} // namespace duckdb
