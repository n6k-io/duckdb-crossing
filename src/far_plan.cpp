#include "internal/far_plan.hpp"

#include "internal/stream_sink.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

CrossingFarSeams &CrossingFarSeams::Of(ClientContext &context) {
	return *context.registered_state->GetOrCreate<CrossingFarSeams>(KEY);
}

namespace {

struct SeamRowsBindData : public TableFunctionData {
	explicit SeamRowsBindData(shared_ptr<CrossingSeamInbox> inbox_p) : inbox(std::move(inbox_p)) {
	}
	shared_ptr<CrossingSeamInbox> inbox;
};

struct SeamRowsGlobalState : public GlobalTableFunctionState {
	explicit SeamRowsGlobalState(idx_t streams) : streams(streams) {
	}
	idx_t streams;
	idx_t MaxThreads() const override {
		return streams;
	}
};

struct SeamRowsLocalState : public LocalTableFunctionState {
	unique_ptr<ColumnDataCollection> rows;
	ColumnDataScanState scan;
	idx_t rank = 0;
	idx_t bytes = 0;
};

unique_ptr<FunctionData> SeamRowsBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto id = input.inputs[0].GetValue<idx_t>();
	auto &seams = CrossingFarSeams::Of(context).inboxes;
	auto found = seams.find(id);
	if (found == seams.end()) {
		throw InvalidInputException("crossing: the plan reads a seam %llu it did not declare", id);
	}
	return_types = found->second->Types();
	for (idx_t c = 0; c < return_types.size(); c++) {
		names.emplace_back("c" + to_string(c));
	}
	return make_uniq<SeamRowsBindData>(found->second);
}

unique_ptr<GlobalTableFunctionState> SeamRowsInitGlobal(ClientContext &, TableFunctionInitInput &input) {
	return make_uniq<SeamRowsGlobalState>(input.bind_data->Cast<SeamRowsBindData>().inbox->Streams());
}

unique_ptr<LocalTableFunctionState> SeamRowsInitLocal(ExecutionContext &, TableFunctionInitInput &,
                                                      GlobalTableFunctionState *) {
	return make_uniq<SeamRowsLocalState>();
}

void SeamRowsScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &local = data.local_state->Cast<SeamRowsLocalState>();
	auto &inbox = *data.bind_data->Cast<SeamRowsBindData>().inbox;
	while (true) {
		if (local.rows) {
			local.rows->Scan(local.scan, output);
			if (output.size() > 0) {
				return;
			}
			local.rows.reset();
		}
		if (!data.interrupt_state) {
			throw InternalException("crossing: a seam scan has nothing to resume it");
		}
		switch (inbox.Take(local.rows, local.rank, local.bytes, *data.interrupt_state)) {
		case CrossingSeamInbox::Took::ROWS:
			local.rows->InitializeScan(local.scan);
			break;
		case CrossingSeamInbox::Took::PARK:
			data.async_result = AsyncResultType::BLOCKED;
			return;
		case CrossingSeamInbox::Took::DONE:
			return;
		}
	}
}

unique_ptr<NodeStatistics> SeamRowsCardinality(ClientContext &, const FunctionData *bind_data) {
	auto &estimate = bind_data->Cast<SeamRowsBindData>().inbox->estimate;
	if (!estimate.IsValid()) {
		return make_uniq<NodeStatistics>();
	}
	return make_uniq<NodeStatistics>(estimate.GetIndex(), estimate.GetIndex());
}

OperatorPartitionData SeamRowsPartition(ClientContext &, TableFunctionGetPartitionInput &input) {
	return OperatorPartitionData(input.local_state->Cast<SeamRowsLocalState>().rank);
}

TableFunction SeamRowsFunction(const char *name, OrderPreservationType order) {
	TableFunction function(name, {LogicalType::UBIGINT}, SeamRowsScan, SeamRowsBind, SeamRowsInitGlobal,
	                       SeamRowsInitLocal);
	function.get_partition_data = SeamRowsPartition;
	function.cardinality = SeamRowsCardinality;
	function.order_preservation_type = order;
	return function;
}

class PhysicalStreamOut : public PhysicalOperator {
public:
	PhysicalStreamOut(PhysicalPlan &physical_plan, vector<LogicalType> types_p, CrossingStreamSink sink_p,
	                  bool writes_p, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
	      sink(std::move(sink_p)), writes(writes_p) {
	}

	CrossingStreamSink sink;
	bool writes;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return sink.parallel;
	}
	bool SinkOrderDependent() const override {
		return sink.OrderDependent();
	}
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return sink.PartitionInfo();
	}
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &) const override {
		return sink.GlobalState();
	}
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &) const override {
		return sink.LocalState();
	}
	SinkResultType Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const override {
		return sink.Sink(chunk, input);
	}
	SinkCombineResultType Combine(ExecutionContext &, OperatorSinkCombineInput &input) const override {
		return sink.Combine(input);
	}
	SinkNextBatchType NextBatch(ExecutionContext &, OperatorSinkNextBatchInput &input) const override {
		return sink.NextBatch(input);
	}
	SinkNextBatchType UpdateMinBatchIndex(ExecutionContext &, OperatorSinkNextBatchInput &input) const override {
		return sink.UpdateMinBatchIndex(input);
	}
	SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &, OperatorSinkFinalizeInput &input) const override {
		sink.Finish(input.global_state);
		return SinkFinalizeType::READY;
	}
	string GetName() const override {
		return "CROSSING_STREAM_OUT";
	}
};

struct FarRootSourceState : public GlobalSourceState {};

class PhysicalFarRoot : public PhysicalOperator {
public:
	PhysicalFarRoot(PhysicalPlan &physical_plan, vector<LogicalType> types_p)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), 1) {
	}

	bool IsSource() const override {
		return true;
	}
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &) const override {
		return make_uniq<FarRootSourceState>();
	}
	SourceResultType GetDataInternal(ExecutionContext &, DataChunk &, OperatorSourceInput &) const override {
		return SourceResultType::FINISHED;
	}
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
		auto &state = meta_pipeline.GetState();
		state.SetPipelineSource(current, *this);
		vector<shared_ptr<Pipeline>> reads;
		shared_ptr<Pipeline> last_write;
		for (auto &child : children) {
			auto &slice = child.get();
			slice.sink_state.reset();
			auto &slice_pipeline = meta_pipeline.CreateChildMetaPipeline(current, slice);
			slice_pipeline.Build(slice.children[0].get());
			auto base = slice_pipeline.GetBasePipeline();
			if (!slice.Cast<PhysicalStreamOut>().writes) {
				reads.push_back(base);
				continue;
			}
			for (auto &read : reads) {
				base->AddDependency(read);
			}
			if (last_write) {
				base->AddDependency(last_write);
			}
			last_write = base;
		}
	}
	vector<const_reference<PhysicalOperator>> GetSources() const override {
		return {*this};
	}
	string GetName() const override {
		return "CROSSING_FAR_ROOT";
	}
};

class LogicalFarOperator : public LogicalExtensionOperator {
public:
	explicit LogicalFarOperator(TableIndex table_index_p) : table_index(table_index_p) {
	}

	TableIndex table_index;

	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, 1);
	}
	vector<TableIndex> GetTableIndex() const override {
		return {table_index};
	}
	string GetExtensionName() const override {
		return "crossing";
	}
	bool SupportSerialization() const override {
		return false;
	}
	void Serialize(Serializer &) const override {
		throw NotImplementedException("crossing: a far plan is built on far, never serialized");
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::BIGINT};
	}
};

class LogicalStreamOut : public LogicalFarOperator {
public:
	LogicalStreamOut(TableIndex table_index_p, CrossingLink &link_p, idx_t streams_p, idx_t slice_p, bool ordered_p,
	                 bool writes_p, unique_ptr<LogicalOperator> plan, shared_ptr<CrossingFailure> failure_p)
	    : LogicalFarOperator(table_index_p), link(link_p), streams(streams_p), slice(slice_p), ordered(ordered_p),
	      writes(writes_p), failure(std::move(failure_p)) {
		plan->ResolveOperatorTypes();
		auto bindings = plan->GetColumnBindings();
		for (idx_t c = 0; c < bindings.size(); c++) {
			expressions.push_back(make_uniq<BoundColumnRefExpression>(plan->types[c], bindings[c]));
		}
		children.push_back(std::move(plan));
	}

	CrossingLink &link;
	idx_t streams;
	idx_t slice;
	bool ordered;
	bool writes = false;
	shared_ptr<CrossingFailure> failure;

	string GetName() const override {
		return "CROSSING_STREAM_OUT";
	}
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		vector<idx_t> columns;
		vector<LogicalType> row_types;
		for (auto &expr : expressions) {
			columns.push_back(expr->Cast<BoundReferenceExpression>().Index());
			row_types.push_back(expr->GetReturnType());
		}
		CrossingStreamSink sink(link, streams, slice, false, std::move(columns), row_types, ordered,
		                        !ordered || PhysicalPlanGenerator::UseBatchIndex(context, child), failure,
		                        ClientConfig::GetConfig(context).max_streaming_buffer_size);
		auto &out =
		    planner.Make<PhysicalStreamOut>(std::move(row_types), std::move(sink), writes, estimated_cardinality);
		out.children.push_back(child);
		return out;
	}
};

class LogicalFarRoot : public LogicalFarOperator {
public:
	using LogicalFarOperator::LogicalFarOperator;

	string GetName() const override {
		return "CROSSING_FAR_ROOT";
	}
	PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
		auto &root = planner.Make<PhysicalFarRoot>(types);
		for (auto &child : children) {
			root.children.push_back(planner.CreatePlan(*child));
		}
		return root;
	}
};

} // namespace

void RegisterCrossingFarFunctions(DatabaseInstance &db) {
	auto &system = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	for (auto &function : {SeamRowsFunction(CROSSING_SEAM_ROWS_FUNCTION, OrderPreservationType::NO_ORDER),
	                       SeamRowsFunction(CROSSING_SEAM_ORDERED_ROWS_FUNCTION, OrderPreservationType::FIXED_ORDER)}) {
		CreateTableFunctionInfo info(function);
		info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		system.CreateFunction(transaction, info);
	}
}

unique_ptr<LogicalOperator> MakeFarRoot(Binder &binder, CrossingLink &link, idx_t streams,
                                        vector<CrossingFarSlice> slices, shared_ptr<CrossingFailure> failure) {
	auto root = make_uniq<LogicalFarRoot>(binder.GenerateTableIndex());
	for (auto &slice : slices) {
		root->children.push_back(make_uniq<LogicalStreamOut>(binder.GenerateTableIndex(), link, streams, slice.id,
		                                                     slice.ordered, slice.writes, std::move(slice.plan),
		                                                     failure));
	}
	return std::move(root);
}

} // namespace duckdb
