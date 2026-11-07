#include "internal/crossing_remote.hpp"

#include "internal/control_frames.hpp"
#include "internal/crossing_read.hpp"
#include "internal/parking.hpp"
#include "internal/seam_inbox.hpp"
#include "internal/stream_sink.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

#include <algorithm>

namespace duckdb {

CrossingRemote::CrossingRemote(CrossingLink &link_p) : link(link_p), control(link.Open(0, &replies)) {
}

CrossingRemote::~CrossingRemote() {
	if (!ended) {
		control->Close(nullptr);
	}
}

ErrorData CrossingRemote::DrainReply(optional_ptr<ClientContext> context) {
	string reply;
	bool replied = false;
	try {
		replied = replies.Next(reply, [&context]() { return context && context->IsInterrupted(); });
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	if (!replied) {
		if (context && context->IsInterrupted()) {
			throw InterruptException();
		}
		return ErrorData(ExceptionType::IO, "crossing: the link closed before far replied");
	}
	try {
		ThrowIfControlError(reply);
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	return ErrorData();
}

ErrorData CrossingRemote::DrainStatement(optional_ptr<ClientContext> context) {
	ErrorData failed;
	while (unread_replies > 0) {
		auto error = DrainReply(context);
		unread_replies--;
		failed = failed.HasError() ? failed : error;
	}
	if (!statement_running) {
		return failed;
	}
	auto error = DrainReply(context);
	statement_running = false;
	return failed.HasError() ? failed : error;
}

void CrossingRemote::End(const char *word) {
	ended = true;
	if (ended_by_far) {
		control->Close(nullptr);
		return;
	}
	auto failed = DrainStatement();
	SendControl(*control, failed.HasError() ? CROSSING_CONTROL_ROLLBACK : word);
	string reply;
	bool replied = replies.Next(reply);
	control->Close(nullptr);
	if (failed.HasError() && word == CROSSING_CONTROL_COMMIT) {
		failed.Throw();
	}
	if (!replied) {
		throw IOException("crossing: the link closed before far ended the transaction");
	}
	ThrowIfControlError(reply);
}

void CrossingRemote::RunDdl(ClientContext &context, const string &frame) {
	Ask(context, CROSSING_CONTROL_DDL + frame);
}

vector<string> CrossingRemote::Tables(ClientContext &context, const string &schema) {
	auto reply = Ask(context, CROSSING_CONTROL_TABLES + schema);
	return reply.empty() ? vector<string>() : StringUtil::Split(reply, '\n');
}

CrossingTable CrossingRemote::Describe(ClientContext &context, const string &schema, const string &name) {
	auto lines = StringUtil::Split(Ask(context, CROSSING_CONTROL_DESCRIBE + schema + "\n" + name), '\n');
	CrossingTable table;
	table.name = name;
	table.row_ids = lines[0] == "table";
	idx_t at = 1;
	auto column_count = std::stoull(lines[at++]);
	for (idx_t c = 0; c < column_count; c++, at += 2) {
		table.Column(lines[at], TransformStringToLogicalType(lines[at + 1], context));
	}
	while (at < lines.size()) {
		auto kind = lines[at++];
		auto named = std::stoull(lines[at++]);
		vector<Identifier> columns;
		for (idx_t c = 0; c < named; c++) {
			columns.emplace_back(lines[at++]);
		}
		if (kind != "NOT NULL") {
			table.constraints.push_back(make_uniq<UniqueConstraint>(std::move(columns), kind == "PRIMARY KEY"));
			continue;
		}
		auto column = std::find(table.column_names.begin(), table.column_names.end(), columns[0].GetIdentifierName());
		table.constraints.push_back(
		    make_uniq<NotNullConstraint>(LogicalIndex(NumericCast<idx_t>(column - table.column_names.begin()))));
	}
	return table;
}

string CrossingRemote::Ask(ClientContext &context, const string &question) {
	DrainStatement(&context);
	used = true;
	SendControl(*control, question);
	string reply;
	if (!replies.Next(reply, [&context]() { return context.IsInterrupted(); })) {
		if (context.IsInterrupted()) {
			unread_replies++;
			throw InterruptException();
		}
		throw IOException("crossing: the link closed before far answered");
	}
	ThrowIfControlError(reply);
	return reply;
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

void CrossingRemoteStatement::Planned(PhysicalOperator &op) {
	lock_guard<mutex> guard(lock);
	planned.push_back(&op);
}

bool CrossingRemoteStatement::WasPlanned(const PhysicalOperator &op) {
	lock_guard<mutex> guard(lock);
	return std::find(planned.begin(), planned.end(), &op) != planned.end();
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

namespace {

string WriteText(const SubstraitWrite &write) {
	auto target = write.schema + "." + write.table;
	if (write.merge) {
		auto text = "MERGE INTO " + target;
		if (write.condition) {
			text += " ON " + write.condition->ToString();
		}
		for (auto &action : write.actions) {
			text += "\n" + StringUtil::Replace(EnumUtil::ToString(action.when), "_", " ") + " THEN " +
			        StringUtil::Replace(EnumUtil::ToString(action.type), "MERGE_", "");
		}
		return text;
	}
	auto text = StringUtil::Upper(CrossingVerbName(write.verb)) + " " + target;
	if (!write.set_columns.empty()) {
		auto columns = StringUtil::Join(write.set_columns, ", ");
		text += write.verb == CrossingVerb::UPDATE ? " SET " + columns : " (" + columns + ")";
	}
	if (write.condition) {
		text += " WHERE " + write.condition->ToString();
	}
	return text;
}

string TypeNames(const vector<LogicalType> &types) {
	vector<string> names;
	for (auto &type : types) {
		names.push_back(type.ToString());
	}
	return StringUtil::Join(names, ", ");
}

} // namespace

InsertionOrderPreservingMap<string> CrossingRemoteStatement::Explain(idx_t slice,
                                                                     optional_ptr<const vector<LogicalType>> seam) {
	lock_guard<mutex> guard(lock);
	InsertionOrderPreservingMap<string> result;
	for (auto &entry : added) {
		if (entry.slice.id != slice) {
			continue;
		}
		vector<string> runs;
		if (entry.slice.write) {
			runs.push_back(WriteText(*entry.slice.write));
		}
		if (auto source = entry.slice.Source()) {
			runs.push_back(source->ToString());
		}
		result["Runs on source"] = StringUtil::Join(runs, "\n");
		if (seam) {
			auto rows = entry.seam_estimate.IsValid() ? "~" + to_string(entry.seam_estimate.GetIndex()) + " rows"
			                                          : string("rows");
			result["Sends"] = rows + " (" + TypeNames(*seam) + ")";
		}
		if (!entry.slice.write) {
			result["Returns"] = "rows";
		} else if (entry.slice.write->returning.empty()) {
			result["Returns"] = "count";
		} else {
			result["Returns"] = "rows (" + StringUtil::Join(entry.slice.write->returning, ", ") + ")";
		}
	}
	return result;
}

void CrossingRemoteStatement::Start() {
	lock_guard<mutex> guard(lock);
	if (!started) {
		started = true;
		try {
			vector<SubstraitSlice> slices;
			CrossingPlanFrame frame;
			frame.streams = remote.Link().Streams();
			bool writes = false;
			for (auto &entry : added) {
				SubstraitSlice slice;
				slice.id = entry.slice.id;
				slice.ordered = entry.slice.ordered;
				slice.seam_ordered = entry.seam_ordered;
				slice.seam_estimate = entry.seam_estimate;
				slice.source = entry.slice.Source();
				slice.write = entry.slice.write.get();
				writes |= entry.slice.write != nullptr;
				slices.push_back(slice);
				frame.slices.push_back(slice.id);
			}
			frame.plan = RenderSubstraitSlices(slices);
			frame.ends_transaction = auto_commit && !writes && !remote.Used();
			remote.StatementSent(frame.ends_transaction);
			SendControl(remote.Control(), frame.Encode());
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
	auto failed = remote.DrainStatement();
	if (failed.HasError()) {
		failed.Throw();
	}
}

namespace {

struct RemoteResults {
	shared_ptr<CrossingSeamInbox> inbox;
	vector<unique_ptr<CrossingInboxReceiver>> receivers;
	vector<unique_ptr<CrossingStream>> streams;
	mutex credit_lock;
	idx_t unsent_credit = 0;

	RemoteResults(ClientContext &context, CrossingLink &link, idx_t slice, const vector<LogicalType> &types,
	              bool ordered) {
		inbox = make_shared_ptr<CrossingSeamInbox>(context, types, ordered, link.Streams());
		for (idx_t k = 0; k < link.Streams(); k++) {
			receivers.push_back(make_uniq<CrossingInboxReceiver>(inbox));
			streams.push_back(link.Open(CrossingSliceStreamId(slice, false, k), receivers.back().get()));
		}
	}

	~RemoteResults() {
		for (auto &stream : streams) {
			stream->Close(nullptr);
		}
	}

	//! Tells far what has been read. False when the stream cannot take it yet; the waker goes when it can.
	bool GiveCredit(idx_t bytes, CrossingWaker waker) {
		lock_guard<mutex> guard(credit_lock);
		unsent_credit += bytes;
		if (unsent_credit == 0) {
			return true;
		}
		CrossingFrame frame;
		frame.header = to_string(unsent_credit);
		if (streams[0]->Send(frame, std::move(waker)) == CrossingIo::WAIT) {
			return false;
		}
		unsent_credit = 0;
		return true;
	}
};

struct RemoteSourceGlobal : public GlobalSourceState {
	unique_ptr<RemoteResults> results;

	idx_t MaxThreads() override {
		return results->streams.size();
	}
};

struct RemoteSourceLocal : public LocalSourceState {
	unique_ptr<ColumnDataCollection> rows;
	ColumnDataScanState scan;
	idx_t rank = 0;
	idx_t bytes = 0;
	DataChunk received;
};

class PhysicalCrossingRemote : public PhysicalOperator {
public:
	PhysicalCrossingRemote(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
	                       shared_ptr<CrossingRemoteStatement> statement_p, idx_t slice_p, bool ordered_p,
	                       CrossingRemoteColumns columns_p, unique_ptr<CrossingStreamSink> sink_p, bool awaits_result_p,
	                       idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types_p), estimated_cardinality),
	      statement(std::move(statement_p)), slice(slice_p), ordered(ordered_p), columns(std::move(columns_p)),
	      sink(std::move(sink_p)), awaits_result(awaits_result_p) {
	}

	shared_ptr<CrossingRemoteStatement> statement;
	idx_t slice;
	bool ordered;
	CrossingRemoteColumns columns;
	unique_ptr<CrossingStreamSink> sink;
	bool awaits_result;
	mutable unique_ptr<RemoteResults> awaited;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override {
		if (meta_pipeline.HasRecursiveCTE()) {
			throw NotImplementedException("crossing: the recursive part of a WITH RECURSIVE cannot read a source's "
			                              "table: its far plan runs once per statement");
		}
		PhysicalOperator::BuildPipelines(current, meta_pipeline);
	}

	InsertionOrderPreservingMap<string> ParamsToString() const override {
		vector<LogicalType> seam;
		if (sink && !children.empty()) {
			seam = children[0].get().GetTypes();
		}
		return statement->Explain(slice, sink ? &seam : nullptr);
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
	SinkNextBatchType UpdateMinBatchIndex(ExecutionContext &, OperatorSinkNextBatchInput &input) const override {
		return sink->UpdateMinBatchIndex(input);
	}
	SinkFinalizeType Finalize(Pipeline &, Event &, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		sink->Finish(input.global_state);
		if (!awaits_result) {
			return SinkFinalizeType::READY;
		}
		if (!awaited) {
			awaited = make_uniq<RemoteResults>(context, statement->Remote().Link(), slice, columns.received, false);
		}
		while (true) {
			unique_ptr<ColumnDataCollection> rows;
			idx_t rank = 0;
			idx_t bytes = 0;
			switch (awaited->inbox->Take(rows, rank, bytes, input.interrupt_state)) {
			case CrossingSeamInbox::Took::DONE:
				return SinkFinalizeType::READY;
			case CrossingSeamInbox::Took::PARK:
				return SinkFinalizeType::BLOCKED;
			default:
				break;
			}
		}
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
		auto state = make_uniq<RemoteSourceGlobal>();
		state->results =
		    make_uniq<RemoteResults>(context, statement->Remote().Link(), slice, columns.received, ordered);
		return std::move(state);
	}
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &, GlobalSourceState &) const override {
		auto state = make_uniq<RemoteSourceLocal>();
		state->received.Initialize(Allocator::DefaultAllocator(), columns.received);
		return std::move(state);
	}

	void Emit(DataChunk &received, DataChunk &chunk) const {
		chunk.SetChildCardinality(received.size());
		for (idx_t c = 0; c < columns.positions.size(); c++) {
			auto position = columns.positions[c];
			if (position == CrossingRemoteColumns::NULL_COLUMN) {
				chunk.data[c].SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(chunk.data[c], true);
			} else {
				chunk.data[c].Reference(received.data[position]);
			}
		}
	}

	SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk, OperatorSourceInput &input) const override {
		auto &global = input.global_state.Cast<RemoteSourceGlobal>();
		auto &local = input.local_state.Cast<RemoteSourceLocal>();
		auto &results = *global.results;
		while (true) {
			if (local.rows) {
				local.received.Reset();
				local.rows->Scan(local.scan, local.received);
				if (local.received.size() > 0) {
					Emit(local.received, chunk);
					return SourceResultType::HAVE_MORE_OUTPUT;
				}
				local.rows.reset();
			}
			auto parking = CrossingParking::Of(input.interrupt_state);
			auto credited = results.GiveCredit(local.bytes, parking->Waker());
			local.bytes = 0;
			if (!credited) {
				annotated_lock_guard<annotated_mutex> guard(global.lock);
				if (parking->Park(global)) {
					return SourceResultType::BLOCKED;
				}
				if (!global.CanBlock()) {
					return SourceResultType::FINISHED;
				}
				continue;
			}
			switch (results.inbox->Take(local.rows, local.rank, local.bytes, input.interrupt_state)) {
			case CrossingSeamInbox::Took::DONE:
				return SourceResultType::FINISHED;
			case CrossingSeamInbox::Took::PARK:
				return SourceResultType::BLOCKED;
			default:
				local.rows->InitializeScan(local.scan);
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
	if (!statement.WasPlanned(plan)) {
		return false;
	}
	auto remote = &plan.Cast<PhysicalCrossingRemote>();
	statement.PutWrite(remote->slice, std::move(write), std::move(owned));
	remote->columns = columns;
	remote->types = types;
	remote->ordered = false;
	return true;
}

unique_ptr<CrossingRemoteSeamSink> CrossingSeamSinkOf(ClientContext &context, PhysicalOperator &feed) {
	auto seam = make_uniq<CrossingRemoteSeamSink>();
	seam->types = feed.GetTypes();
	seam->ordered = PhysicalPlanGenerator::PreserveInsertionOrder(context, feed);
	seam->parallel = !seam->ordered || PhysicalPlanGenerator::UseBatchIndex(context, feed);
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
	if (seam) {
		awaits_result = seam->awaits_result;
		vector<idx_t> positions;
		for (idx_t c = 0; c < seam->types.size(); c++) {
			positions.push_back(c);
		}
		seam_ordered = seam->ordered;
		sink = make_uniq<CrossingStreamSink>(statement->Remote().Link(), statement->Remote().Link().Streams(), slice.id,
		                                     true, std::move(positions), std::move(seam->types), seam->ordered,
		                                     seam->parallel, nullptr);
	}
	auto slice_id = slice.id;
	auto ordered = slice.ordered;
	statement->Add(std::move(slice), seam_ordered, seam_estimate);
	auto &remote =
	    planner.Make<PhysicalCrossingRemote>(std::move(types), statement, slice_id, ordered, std::move(columns),
	                                         std::move(sink), awaits_result, estimated_cardinality);
	statement->Planned(remote);
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
