#include "internal/crossing_pass.hpp"

#include "internal/crossing_read.hpp"
#include "internal/crossing_shape.hpp"
#include "internal/crossing_write.hpp"
#include "internal/pass.hpp"

#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

void CrossingMoveWorkPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &binder = input.optimizer.binder;
	FreshTableIndex fresh = [&binder]() {
		return binder.GenerateTableIndex();
	};
	ResolveKeyAliases(*plan);
	WidenKeyedWritesForReturning(*plan);
	MoveCrossableWorkIntoFragments(plan, fresh);
	ShapeWrites(input.context, plan, fresh);
}

namespace {

void SwapCrossingGetsForTheirNodes(ClientContext &context, unique_ptr<LogicalOperator> &node) {
	for (auto &child : node->children) {
		SwapCrossingGetsForTheirNodes(context, child);
	}
	if (node->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = node->Cast<LogicalGet>();
	if (dynamic_cast<CrossingScanBindData *>(get.bind_data.get())) {
		if (!get.table_filters.filters.empty() || !get.projection_ids.empty() || !get.projected_input.empty()) {
			throw InternalException("crossing: a scan of '%s' was optimized in a way the read cannot carry",
			                        get.bind_data->Cast<CrossingScanBindData>().source_table);
		}
		get.ResolveOperatorTypes();
		auto cardinality = get.EstimateCardinality(context);
		auto bind_data = unique_ptr_cast<FunctionData, CrossingScanBindData>(std::move(get.bind_data));
		auto read =
		    make_uniq<LogicalCrossingRead>(get.table_index, get.GetColumnIds(), get.types, std::move(bind_data));
		read->SetEstimatedCardinality(cardinality);
		node = std::move(read);
		return;
	}
	if (dynamic_cast<CrossingWriteBindData *>(get.bind_data.get())) {
		auto bind_data = unique_ptr_cast<FunctionData, CrossingWriteBindData>(std::move(get.bind_data));
		auto write = make_uniq<LogicalCrossingWholeWrite>(get.table_index, std::move(bind_data));
		write->SetEstimatedCardinality(1);
		node = std::move(write);
	}
}

} // namespace

void CrossingNarrowPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	NarrowFragmentsToTheirScans(plan);
	SwapCrossingGetsForTheirNodes(input.context, plan);
}

} // namespace duckdb
