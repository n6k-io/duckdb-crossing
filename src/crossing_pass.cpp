#include "internal/crossing_pass.hpp"

#include "internal/crossing_fill.hpp"
#include "internal/crossing_read.hpp"
#include "internal/crossing_table_entry.hpp"
#include "internal/crossing_write.hpp"
#include "internal/pass.hpp"

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
		if (!get.table_filters.filters.empty() || !get.projection_ids.empty() || !get.projected_input.empty()) {
			throw InternalException("crossing: the scan of '%s' was optimized in a way the read cannot carry",
			                        data->SourceTable());
		}
		get.ResolveOperatorTypes();
		auto cardinality = get.EstimateCardinality(context);
		auto bind_data = unique_ptr_cast<FunctionData, CrossingBindData>(std::move(get.bind_data));
		auto read =
		    make_uniq<LogicalCrossingRead>(get.table_index, get.GetColumnIds(), get.types, std::move(bind_data));
		read->SetEstimatedCardinality(cardinality);
		node = std::move(read);
		return;
	}
	auto bind_data = unique_ptr_cast<FunctionData, CrossingBindData>(std::move(get.bind_data));
	auto fence = make_uniq<LogicalCrossingFence>(get.table_index, std::move(bind_data));
	fence->SetEstimatedCardinality(1);
	node = std::move(fence);
}

} // namespace

void CrossingFoldPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &identity = IdentityOf(input);
	auto &binder = input.optimizer.binder;
	FreshTableIndex fresh = [&binder]() {
		return binder.GenerateTableIndex();
	};
	ResolveKeyAliases(*plan, identity);
	AddRowImageForReturning(*plan, identity);
	FoldCrossableWorkIntoFragments(plan, fresh, identity);
	FillWrites(input.context, plan, fresh, identity);
}

void CrossingNarrowPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &identity = IdentityOf(input);
	NarrowScansToRequestedColumns(plan, identity);
	SwapCrossingGetsForTheirNodes(input.context, plan, identity);
}

} // namespace duckdb
