#include "internal/crossing_remote.hpp"

#include "internal/control_frames.hpp"
#include "internal/crossing_read.hpp"
#include "internal/key_check.hpp"
#include "internal/parking.hpp"
#include "internal/seam_inbox.hpp"
#include "internal/stream_sink.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
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

void CrossingRemoteStatement::PutWrite(idx_t slice, unique_ptr<SubstraitWrite> write,
                                       vector<unique_ptr<Expression>> owned) {
	lock_guard<mutex> guard(lock);
	for (auto &entry : added) {
		if (entry.slice.id == slice) {
			entry.slice.write = std::move(write);
			entry.slice.owned = std::move(owned);
			entry.slice.ordered = false;
			return;
		}
	}
	throw InternalException("crossing: no slice %llu to write through", slice);
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
				slice.source = entry.slice.Source();
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
	atomic<idx_t> rows_emitted {0};
	atomic<idx_t> affected {0};

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
	DataChunk received;
};

struct RemoteSinkGlobal : public GlobalSinkState {
	unique_ptr<GlobalSinkState> streams;
	value_set_t keys;

	idx_t MaxThreads(idx_t source_max_threads) override {
		return streams->MaxThreads(source_max_threads);
	}
};

struct RemoteSinkLocal : public LocalSinkState {
	unique_ptr<LocalSinkState> streams;
	value_set_t keys;
};

void NoteKeys(value_set_t &keys, DataChunk &chunk, const vector<idx_t> &key_positions) {
	for (idx_t r = 0; r < chunk.size(); r++) {
		child_list_t<Value> key;
		for (idx_t k = 0; k < key_positions.size(); k++) {
			key.emplace_back(to_string(k), chunk.GetValue(key_positions[k], r));
		}
		keys.insert(Value::STRUCT(std::move(key)));
	}
}

class PhysicalCrossingRemote : public PhysicalOperator {
public:
	PhysicalCrossingRemote(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
	                       shared_ptr<CrossingRemoteStatement> statement_p, idx_t slice_p, bool ordered_p,
	                       CrossingRemoteColumns columns_p, unique_ptr<CrossingStreamSink> sink_p,
	                       CrossingRemoteKeyCheck keys_p, bool awaits_result_p, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
	      statement(std::move(statement_p)), slice(slice_p), ordered(ordered_p), columns(std::move(columns_p)),
	      sink(std::move(sink_p)), keys(std::move(keys_p)), awaits_result(awaits_result_p) {
	}

	shared_ptr<CrossingRemoteStatement> statement;
	idx_t slice;
	bool ordered;
	CrossingRemoteColumns columns;
	unique_ptr<CrossingStreamSink> sink;
	CrossingRemoteKeyCheck keys;
	bool awaits_result;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
		if (meta_pipeline.HasRecursiveCTE()) {
			throw NotImplementedException("crossing: the recursive part of a WITH RECURSIVE cannot read a source's "
			                              "table: its far plan runs once per statement");
		}
		PhysicalOperator::BuildPipelines(current, meta_pipeline);
	}

	void AwaitResult(RemoteSinkGlobal &global) const {
		auto &link = statement->Remote().Link();
		idx_t affected = 0;
		for (idx_t k = 0; k < link.Streams(); k++) {
			auto stream = link.Open(CrossingSliceStreamId(slice, false, k));
			CrossingFrame frame;
			frame.rows.Initialize(Allocator::DefaultAllocator(), columns.received);
			while (true) {
				auto signal = make_shared_ptr<InterruptDoneSignalState>();
				auto parking = CrossingParking::Of(InterruptState(signal));
				frame.rows.Reset();
				auto io = stream->Recv(frame, parking->Waker());
				if (io == CrossingIo::DONE) {
					break;
				}
				if (io == CrossingIo::WAIT) {
					if (parking->Park()) {
						signal->Await();
					}
					continue;
				}
				if (frame.header == CROSSING_IDLE_HEADER) {
					continue;
				}
				for (idx_t r = 0; r < frame.rows.size(); r++) {
					affected += NumericCast<idx_t>(frame.rows.GetValue(0, r).GetValue<int64_t>());
				}
			}
			stream->Close(nullptr);
		}
		if (!keys.key_positions.empty()) {
			ThrowIfKeyNotUnique(affected, global.keys.size(), keys.table, keys.key_columns, keys.verb.c_str());
		}
	}

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
		auto state = make_uniq<RemoteSinkGlobal>();
		state->streams = sink->GlobalState();
		return std::move(state);
	}
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &) const override {
		auto state = make_uniq<RemoteSinkLocal>();
		state->streams = sink->LocalState();
		return std::move(state);
	}
	SinkResultType Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSinkGlobal>();
		auto &local = input.local_state.Cast<RemoteSinkLocal>();
		local.streams->partition_info = local.partition_info;
		OperatorSinkInput inner {*global.streams, *local.streams, input.interrupt_state};
		auto result = sink->Sink(chunk, inner);
		if (result == SinkResultType::NEED_MORE_INPUT && !keys.key_positions.empty()) {
			NoteKeys(local.keys, chunk, keys.key_positions);
		}
		return result;
	}
	SinkCombineResultType Combine(ExecutionContext &, OperatorSinkCombineInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSinkGlobal>();
		auto &local = input.local_state.Cast<RemoteSinkLocal>();
		local.streams->partition_info = local.partition_info;
		OperatorSinkCombineInput inner {*global.streams, *local.streams, input.interrupt_state};
		auto result = sink->Combine(inner);
		if (result == SinkCombineResultType::FINISHED && !local.keys.empty()) {
			annotated_lock_guard<annotated_mutex> guard(global.lock);
			global.keys.insert(local.keys.begin(), local.keys.end());
			local.keys.clear();
		}
		return result;
	}
	SinkNextBatchType NextBatch(ExecutionContext &, OperatorSinkNextBatchInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSinkGlobal>();
		auto &local = input.local_state.Cast<RemoteSinkLocal>();
		local.streams->partition_info = local.partition_info;
		OperatorSinkNextBatchInput inner {*global.streams, *local.streams, input.interrupt_state};
		return sink->NextBatch(inner);
	}
	SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &, OperatorSinkFinalizeInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSinkGlobal>();
		sink->Finish(*global.streams);
		if (awaits_result) {
			AwaitResult(global);
		}
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
		state->inbox = make_shared_ptr<CrossingSeamInbox>(context, columns.received, ordered, link.Streams());
		for (idx_t k = 0; k < link.Streams(); k++) {
			state->streams.push_back(link.Open(CrossingSliceStreamId(slice, false, k)));
		}
		return std::move(state);
	}
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &, GlobalSourceState &) const override {
		auto state = make_uniq<RemoteSourceLocal>();
		state->frame.rows.Initialize(Allocator::DefaultAllocator(), columns.received);
		state->received.Initialize(Allocator::DefaultAllocator(), columns.received);
		return std::move(state);
	}

	void Emit(RemoteSourceGlobal &global, DataChunk &received, DataChunk &chunk) const {
		chunk.SetChildCardinality(received.size());
		for (idx_t c = 0; c < columns.positions.size(); c++) {
			auto position = columns.positions[c];
			if (position == CrossingRemoteColumns::ROW_ID) {
				auto row_ids = FlatVector::GetDataMutable<row_t>(chunk.data[c]);
				auto first = global.rows_emitted.fetch_add(received.size());
				for (idx_t r = 0; r < received.size(); r++) {
					row_ids[r] = static_cast<row_t>(first + r);
				}
			} else if (position == CrossingRemoteColumns::NULL_COLUMN) {
				chunk.data[c].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(chunk.data[c], true);
			} else {
				chunk.data[c].Reference(received.data[position]);
			}
		}
	}

	void CheckKeys(RemoteSourceGlobal &global) const {
		if (keys.key_positions.empty() || !sink_state) {
			return;
		}
		auto &sunk = sink_state->Cast<RemoteSinkGlobal>();
		ThrowIfKeyNotUnique(global.affected.load(), sunk.keys.size(), keys.table, keys.key_columns, keys.verb.c_str());
	}

	void NoteAffected(RemoteSourceGlobal &global, DataChunk &rows) const {
		if (keys.key_positions.empty()) {
			return;
		}
		if (keys.returning) {
			global.affected += rows.size();
			return;
		}
		for (idx_t r = 0; r < rows.size(); r++) {
			global.affected += NumericCast<idx_t>(rows.GetValue(0, r).GetValue<int64_t>());
		}
	}

	SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk, OperatorSourceInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSourceGlobal>();
		auto &local = input.local_state.Cast<RemoteSourceLocal>();
		auto &inbox = *global.inbox;
		while (true) {
			if (local.rows) {
				local.received.Reset();
				local.rows->Scan(local.scan, local.received);
				if (local.received.size() > 0) {
					Emit(global, local.received, chunk);
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
					CheckKeys(global);
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
					NoteAffected(global, local.frame.rows);
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
		optional_ptr<PhysicalOperator> feed;
		unique_ptr<CrossingRemoteSeamSink> seam;
		optional_idx estimate;
		if (!children.empty()) {
			feed = planner.CreatePlan(*children[0]);
			seam = CrossingSeamSinkOf(context, *feed);
			estimate = feed->estimated_cardinality;
		}
		return PlanCrossingRemote(context, planner, output_types, statement, std::move(slice),
		                          CrossingRemoteColumns::Identity(output_types), std::move(seam), feed, estimate,
		                          estimated_cardinality);
	}

protected:
	void ResolveTypes() override {
		types = output_types;
	}
};

} // namespace

CrossingRemoteColumns CrossingRemoteColumns::Identity(const vector<LogicalType> &types) {
	CrossingRemoteColumns columns;
	columns.received = types;
	for (idx_t c = 0; c < types.size(); c++) {
		columns.positions.push_back(c);
	}
	return columns;
}

bool TryPutWriteOnRemote(PhysicalOperator &plan, CrossingRemoteStatement &statement, unique_ptr<SubstraitWrite> &write,
                         vector<unique_ptr<Expression>> &owned, const CrossingRemoteColumns &columns,
                         const vector<LogicalType> &types) {
	auto remote = dynamic_cast<PhysicalCrossingRemote *>(&plan);
	if (!remote || remote->statement.get() != &statement) {
		return false;
	}
	statement.PutWrite(remote->slice, std::move(write), std::move(owned));
	remote->columns = columns;
	remote->types = types;
	remote->ordered = false;
	return true;
}

unique_ptr<CrossingRemoteSeamSink> CrossingSeamSinkOf(ClientContext &context, PhysicalOperator &feed,
                                                      CrossingRemoteKeyCheck keys) {
	auto seam = make_uniq<CrossingRemoteSeamSink>();
	seam->types = feed.GetTypes();
	seam->ordered = PhysicalPlanGenerator::PreserveInsertionOrder(context, feed);
	seam->parallel = !seam->ordered || PhysicalPlanGenerator::UseBatchIndex(context, feed);
	seam->keys = std::move(keys);
	return seam;
}

PhysicalOperator &PlanCrossingRemote(ClientContext &, PhysicalPlanGenerator &planner, vector<LogicalType> types,
                                     shared_ptr<CrossingRemoteStatement> statement, CrossingRemoteSlice slice,
                                     CrossingRemoteColumns columns, unique_ptr<CrossingRemoteSeamSink> seam,
                                     optional_ptr<PhysicalOperator> feed, optional_idx seam_estimate,
                                     idx_t estimated_cardinality) {
	unique_ptr<CrossingStreamSink> sink;
	bool seam_ordered = false;
	bool awaits_result = false;
	CrossingRemoteKeyCheck keys;
	if (seam) {
		awaits_result = seam->awaits_result;
		vector<idx_t> positions;
		for (idx_t c = 0; c < seam->types.size(); c++) {
			positions.push_back(c);
		}
		seam_ordered = seam->ordered;
		keys = std::move(seam->keys);
		sink = make_uniq<CrossingStreamSink>(statement->Remote().Link(), slice.id, true, std::move(positions),
		                                     std::move(seam->types), seam->ordered, seam->parallel, nullptr);
	}
	auto slice_id = slice.id;
	auto ordered = slice.ordered;
	statement->Add(std::move(slice), seam_ordered, seam_estimate);
	auto &remote =
	    planner.Make<PhysicalCrossingRemote>(std::move(types), statement, slice_id, ordered, std::move(columns),
	                                         std::move(sink), std::move(keys), awaits_result, estimated_cardinality);
	if (feed) {
		remote.children.push_back(*feed);
	}
	return remote;
}

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
