#include "internal/crossing_remote.hpp"

#include "internal/control_frames.hpp"
#include "internal/crossing_read.hpp"
#include "internal/parking.hpp"
#include "internal/seam_inbox.hpp"
#include "internal/stream_sink.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

CrossingRemote::CrossingRemote(CrossingLink &link_p) : link(link_p), control(link.Open(0)) {
}

CrossingRemote::~CrossingRemote() {
	if (!ended) {
		control->Close(nullptr);
	}
}

ErrorData CrossingRemote::DrainStatement() {
	if (!statement_running) {
		return ErrorData();
	}
	statement_running = false;
	string reply;
	try {
		if (!RecvControl(*control, reply)) {
			throw IOException("crossing: the link closed before far ended the statement");
		}
		ThrowIfControlError(reply);
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

void CrossingRemote::End(const char *word) {
	ended = true;
	auto failed = DrainStatement();
	SendControl(*control, failed.HasError() ? CROSSING_CONTROL_ROLLBACK : word);
	string reply;
	bool replied = RecvControl(*control, reply);
	control->Close(nullptr);
	if (failed.HasError() && word == CROSSING_CONTROL_COMMIT) {
		failed.Throw();
	}
	if (!replied) {
		throw IOException("crossing: the link closed before far ended the transaction");
	}
	ThrowIfControlError(reply);
}

void CrossingRemote::Commit() {
	End(CROSSING_CONTROL_COMMIT);
}

void CrossingRemote::Rollback() {
	End(CROSSING_CONTROL_ROLLBACK);
}

CrossingRemoteStatement::CrossingRemoteStatement(CrossingRemote &remote_p) : remote(remote_p) {
}

void CrossingRemoteStatement::Add(CrossingRemoteSlice slice, bool seam_ordered, optional_idx seam_estimate) {
	lock_guard<mutex> guard(lock);
	Added entry;
	entry.slice = std::move(slice);
	entry.seam_ordered = seam_ordered;
	entry.seam_estimate = seam_estimate;
	added.push_back(std::move(entry));
}

void CrossingRemoteStatement::Start() {
	lock_guard<mutex> guard(lock);
	if (!started) {
		started = true;
		try {
			vector<SubstraitSlice> slices;
			for (auto &entry : added) {
				SubstraitSlice slice;
				slice.id = entry.slice.id;
				slice.ordered = entry.slice.ordered;
				slice.seam_ordered = entry.seam_ordered;
				slice.seam_estimate = entry.seam_estimate;
				slice.source = entry.slice.source.get();
				slice.write = entry.slice.write.get();
				slices.push_back(slice);
			}
			SendControl(remote.Control(), CROSSING_CONTROL_PLAN + RenderSubstraitSlices(slices));
			string reply;
			if (!RecvControl(remote.Control(), reply)) {
				throw IOException("crossing: the link closed before far bound the plan");
			}
			ThrowIfControlError(reply);
			remote.StatementRunning(true);
		} catch (std::exception &ex) {
			start_error = ErrorData(ex);
			finished = true;
		}
	}
	if (start_error.HasError()) {
		start_error.Throw();
	}
}

void CrossingRemoteStatement::Finish() {
	lock_guard<mutex> guard(lock);
	if (finished || !started) {
		return;
	}
	finished = true;
	remote.StatementRunning(false);
	string reply;
	if (!RecvControl(remote.Control(), reply)) {
		throw IOException("crossing: the link closed before far ended the statement");
	}
	ThrowIfControlError(reply);
}

namespace {

struct RemoteSourceGlobal : public GlobalSourceState {
	shared_ptr<CrossingSeamInbox> inbox;
	vector<unique_ptr<CrossingStream>> streams;
	atomic<idx_t> next_stream {0};

	~RemoteSourceGlobal() override {
		for (auto &stream : streams) {
			stream->Close(nullptr);
		}
	}
	idx_t MaxThreads() override {
		return streams.size();
	}
};

struct RemoteSourceLocal : public LocalSourceState {
	optional_idx stream;
	unique_ptr<ColumnDataCollection> rows;
	ColumnDataScanState scan;
	idx_t rank = 0;
	CrossingFrame frame;
};

class PhysicalCrossingRemote : public PhysicalOperator {
public:
	PhysicalCrossingRemote(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
	                       shared_ptr<CrossingRemoteStatement> statement_p, idx_t slice_p, bool ordered_p,
	                       unique_ptr<CrossingStreamSink> sink_p, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
	      statement(std::move(statement_p)), slice(slice_p), ordered(ordered_p), sink(std::move(sink_p)) {
	}

	shared_ptr<CrossingRemoteStatement> statement;
	idx_t slice;
	bool ordered;
	unique_ptr<CrossingStreamSink> sink;

	bool IsSink() const override {
		return sink != nullptr;
	}
	bool ParallelSink() const override {
		return sink && sink->parallel;
	}
	bool SinkOrderDependent() const override {
		return sink && sink->OrderDependent();
	}
	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return sink ? sink->PartitionInfo() : OperatorPartitionInfo::NoPartitionInfo();
	}
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &) const override {
		statement->Start();
		return sink->GlobalState();
	}
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &) const override {
		return sink->LocalState();
	}
	SinkResultType Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const override {
		return sink->Sink(chunk, input);
	}
	SinkCombineResultType Combine(ExecutionContext &, OperatorSinkCombineInput &input) const override {
		return sink->Combine(input);
	}
	SinkNextBatchType NextBatch(ExecutionContext &, OperatorSinkNextBatchInput &input) const override {
		return sink->NextBatch(input);
	}
	SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &, OperatorSinkFinalizeInput &input) const override {
		sink->Finish(input.global_state);
		return SinkFinalizeType::READY;
	}

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}
	OrderPreservationType SourceOrder() const override {
		return ordered ? OrderPreservationType::FIXED_ORDER : OrderPreservationType::NO_ORDER;
	}
	bool SupportsPartitioning(const OperatorPartitionInfo &partition_info) const override {
		return !partition_info.RequiresPartitionColumns();
	}
	OperatorPartitionData GetPartitionData(ExecutionContext &, DataChunk &, GlobalSourceState &,
	                                       LocalSourceState &lstate, const OperatorPartitionInfo &) const override {
		return OperatorPartitionData(lstate.Cast<RemoteSourceLocal>().rank);
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		statement->Start();
		auto &link = statement->Remote().Link();
		auto state = make_uniq<RemoteSourceGlobal>();
		state->inbox = make_shared_ptr<CrossingSeamInbox>(context, types, ordered, link.Streams());
		for (idx_t k = 0; k < link.Streams(); k++) {
			state->streams.push_back(link.Open(CrossingSliceStreamId(slice, false, k)));
		}
		return std::move(state);
	}
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &, GlobalSourceState &) const override {
		auto state = make_uniq<RemoteSourceLocal>();
		state->frame.rows.Initialize(Allocator::DefaultAllocator(), types);
		return std::move(state);
	}

	SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk, OperatorSourceInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSourceGlobal>();
		auto &local = input.local_state.Cast<RemoteSourceLocal>();
		auto &inbox = *global.inbox;
		while (true) {
			if (local.rows) {
				local.rows->Scan(local.scan, chunk);
				if (chunk.size() > 0) {
					return SourceResultType::HAVE_MORE_OUTPUT;
				}
				local.rows.reset();
			}
			if (inbox.TryTake(local.rows, local.rank)) {
				local.rows->InitializeScan(local.scan);
				continue;
			}
			if (!local.stream.IsValid()) {
				auto claimed = global.next_stream.fetch_add(1);
				if (claimed < global.streams.size()) {
					local.stream = claimed;
				}
			}
			if (!local.stream.IsValid()) {
				auto took = inbox.Take(local.rows, local.rank, input.interrupt_state);
				if (took == CrossingSeamInbox::Took::DONE) {
					return SourceResultType::FINISHED;
				}
				if (took == CrossingSeamInbox::Took::PARK) {
					return SourceResultType::BLOCKED;
				}
				local.rows->InitializeScan(local.scan);
				continue;
			}
			auto index = local.stream.GetIndex();
			auto parking = CrossingParking::Of(input.interrupt_state);
			local.frame.rows.Reset();
			switch (global.streams[index]->Recv(local.frame, parking->Waker())) {
			case CrossingIo::ROWS:
				if (local.frame.header == CROSSING_IDLE_HEADER) {
					inbox.Idle(index);
				} else {
					inbox.Append(index, local.frame.rows, std::stoull(local.frame.header));
				}
				break;
			case CrossingIo::DONE:
				inbox.StreamDone(index);
				local.stream = optional_idx();
				break;
			default: {
				SourceResultType parked;
				if (ParkSource(*parking, global, parked)) {
					return parked;
				}
			}
			}
		}
	}

	string GetName() const override {
		return "CROSSING_REMOTE";
	}
};

class LogicalCrossingRemote : public LogicalExtensionOperator {
public:
	LogicalCrossingRemote(TableIndex table_index_p, vector<LogicalType> output_types_p,
	                      shared_ptr<CrossingRemoteStatement> statement_p, CrossingRemoteSlice slice_p,
	                      unique_ptr<LogicalOperator> feed)
	    : table_index(table_index_p), output_types(std::move(output_types_p)), statement(std::move(statement_p)),
	      slice(std::move(slice_p)) {
		if (feed) {
			children.push_back(std::move(feed));
		}
	}

	TableIndex table_index;
	vector<LogicalType> output_types;
	shared_ptr<CrossingRemoteStatement> statement;
	CrossingRemoteSlice slice;

	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, output_types.size());
	}
	vector<TableIndex> GetTableIndex() const override {
		return {table_index};
	}
	string GetName() const override {
		return "CROSSING_REMOTE";
	}
	string GetExtensionName() const override {
		return "crossing";
	}
	bool SupportSerialization() const override {
		return false;
	}
	void Serialize(Serializer &) const override {
		throw NotImplementedException("crossing: a remote node is planned, never serialized");
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		optional_ptr<PhysicalOperator> child;
		unique_ptr<CrossingStreamSink> sink;
		bool seam_ordered = false;
		optional_idx seam_estimate;
		auto &link = statement->Remote().Link();
		if (!children.empty()) {
			child = planner.CreatePlan(*children[0]);
			seam_ordered = PhysicalPlanGenerator::PreserveInsertionOrder(context, *child);
			bool parallel = !seam_ordered || PhysicalPlanGenerator::UseBatchIndex(context, *child);
			seam_estimate = child->estimated_cardinality;
			auto feed_types = child->GetTypes();
			vector<idx_t> columns;
			for (idx_t c = 0; c < feed_types.size(); c++) {
				columns.push_back(c);
			}
			sink = make_uniq<CrossingStreamSink>(link, slice.id, true, std::move(columns), std::move(feed_types),
			                                     seam_ordered, parallel, nullptr);
		}
		auto slice_id = slice.id;
		auto ordered = slice.ordered;
		statement->Add(std::move(slice), seam_ordered, seam_estimate);
		auto &remote = planner.Make<PhysicalCrossingRemote>(output_types, statement, slice_id, ordered, std::move(sink),
		                                                    estimated_cardinality);
		if (child) {
			remote.children.push_back(*child);
		}
		return remote;
	}

protected:
	void ResolveTypes() override {
		types = output_types;
	}
};

} // namespace

unique_ptr<LogicalOperator> MakeCrossingRemote(TableIndex table_index, vector<LogicalType> types,
                                               shared_ptr<CrossingRemoteStatement> statement, CrossingRemoteSlice slice,
                                               unique_ptr<LogicalOperator> feed) {
	if (slice.source) {
		slice.source->ResolveOperatorTypes();
	}
	return make_uniq<LogicalCrossingRemote>(table_index, std::move(types), std::move(statement), std::move(slice),
	                                        std::move(feed));
}

} // namespace duckdb
