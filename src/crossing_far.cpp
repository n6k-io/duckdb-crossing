#include "crossing_far.hpp"

#include "internal/control_frames.hpp"
#include "internal/execute.hpp"
#include "internal/far_plan.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/alter_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <atomic>
#include <thread>

namespace duckdb {

namespace {

struct SeamStream {
	shared_ptr<CrossingSeamInbox> inbox;
	idx_t stream = 0;
	unique_ptr<CrossingStream> wire;
	bool done = false;
};

//! Moves seam frames into their inboxes until the rows end or the statement does: the inboxes take every
//! row, so no frame waits on the plan.
class SeamPump {
public:
	SeamPump(CrossingLink &link, const map<idx_t, shared_ptr<CrossingSeamInbox>> &inboxes) {
		for (auto &entry : inboxes) {
			for (idx_t k = 0; k < entry.second->Streams(); k++) {
				SeamStream stream;
				stream.inbox = entry.second;
				stream.stream = k;
				stream.wire = link.Open(CrossingSliceStreamId(entry.first, true, k));
				streams.push_back(std::move(stream));
			}
		}
	}

	void Run() {
		while (!statement_ended.load()) {
			auto signal = make_shared_ptr<InterruptDoneSignalState>();
			auto parking = CrossingParking::Of(InterruptState(signal));
			{
				lock_guard<mutex> guard(lock);
				current = parking;
			}
			if (statement_ended.load()) {
				return;
			}
			bool progressed = false;
			bool open = false;
			for (auto &stream : streams) {
				if (stream.done) {
					continue;
				}
				progressed |= PullOne(stream, *parking);
				open |= !stream.done;
			}
			if (!open) {
				return;
			}
			if (!progressed && parking->Park()) {
				signal->Await();
			}
		}
	}

	void StatementEnded() {
		statement_ended = true;
		shared_ptr<CrossingParking> parking;
		{
			lock_guard<mutex> guard(lock);
			parking = current;
		}
		if (parking) {
			parking->Waker().Wake();
		}
	}

	void Close(const ErrorData &error) {
		for (auto &stream : streams) {
			stream.wire->Close(error.HasError() ? &error : nullptr);
		}
	}

private:
	static bool PullOne(SeamStream &stream, CrossingParking &parking) {
		CrossingFrame frame;
		frame.rows.Initialize(Allocator::DefaultAllocator(), stream.inbox->Types());
		try {
			switch (stream.wire->Recv(frame, parking.Waker())) {
			case CrossingIo::ROWS:
				if (frame.header == CROSSING_IDLE_HEADER) {
					stream.inbox->Idle(stream.stream);
				} else {
					stream.inbox->Append(stream.stream, frame.rows, std::stoull(frame.header));
				}
				return true;
			case CrossingIo::DONE:
				stream.done = true;
				stream.inbox->StreamDone(stream.stream);
				return true;
			default:
				return false;
			}
		} catch (std::exception &ex) {
			stream.done = true;
			stream.inbox->Cancel(ErrorData(ex));
			return true;
		}
	}

	vector<SeamStream> streams;
	std::atomic<bool> statement_ended {false};
	mutex lock;
	shared_ptr<CrossingParking> current;
};

} // namespace

CrossingFar::CrossingFar(Connection &connection_p, string catalog_p, CrossingFarOptions options_p)
    : connection(connection_p), catalog(std::move(catalog_p)), options(std::move(options_p)) {
	RegisterCrossingFarFunctions(*connection.context->db);
}

void CrossingFar::Cancel(const string &reason) {
	ErrorData error(ExceptionType::INTERRUPT, reason);
	shared_ptr<CrossingParking> waiting;
	shared_ptr<CrossingFailure> failure;
	vector<shared_ptr<CrossingSeamInbox>> seams;
	{
		lock_guard<mutex> guard(lock);
		cancelled = error;
		waiting = control_wait;
		failure = running;
		seams = inboxes;
	}
	if (failure) {
		failure->Cancel(error);
	}
	for (auto &inbox : seams) {
		inbox->Cancel(error);
	}
	if (waiting) {
		waiting->Waker().Wake();
	}
	connection.Interrupt();
}

bool CrossingFar::RunStatement(CrossingLink &link, CrossingStream &control, const string &body) {
	auto &context = *connection.context;
	auto frame = CrossingPlanFrame::Decode(body);
	auto failure = make_shared_ptr<CrossingFailure>();
	auto &seams = CrossingFarSeams::Of(context).inboxes;
	seams.clear();

	auto end_transaction = [&](const ErrorData &error) {
		if (!frame.ends_transaction) {
			SendControl(control, error.HasError() ? ControlError(error) : string(CROSSING_CONTROL_DONE));
			return false;
		}
		connection.Query(error.HasError() ? "ROLLBACK" : "COMMIT");
		control.Close(nullptr);
		return true;
	};

	unique_ptr<Planner> planner;
	vector<shared_ptr<Binder>> slice_binders;
	unique_ptr<QueryResult> result;
	try {
		{
			lock_guard<mutex> guard(lock);
			if (cancelled.HasError()) {
				cancelled.Throw();
			}
			running = failure;
		}
		auto decoded = DecodeSubstraitSlices(connection, catalog, frame.plan, options.catalogs, options.floors);
		for (auto &seam : decoded.seams) {
			auto inbox = make_shared_ptr<CrossingSeamInbox>(context, seam.types, seam.ordered, frame.streams);
			inbox->estimate = seam.estimate;
			seams[seam.id] = inbox;
			lock_guard<mutex> guard(lock);
			inboxes.push_back(std::move(inbox));
		}
		planner = make_uniq<Planner>(context);
		vector<CrossingFarSlice> slices;
		for (auto &slice : decoded.slices) {
			if (!slice.write_catalog.empty()) {
				auto &attached = Catalog::GetCatalog(context, Identifier(slice.write_catalog)).GetAttached();
				MetaTransaction::Get(context).ModifyDatabase(attached, DatabaseModificationType::UPDATE_DATA);
			}
			CrossingFarSlice far_slice;
			far_slice.id = slice.id;
			far_slice.ordered = slice.ordered;
			far_slice.writes = !slice.write_catalog.empty();
			auto binder = Binder::CreateBinder(context, planner->binder.get());
			auto bound = binder->Bind(*slice.statement);
			if (!slice.returning_types.empty() && bound.types != slice.returning_types) {
				throw CatalogException("crossing: the write's table changed on the source: it returns other columns "
				                       "than the description taken at attach says; refresh the attach");
			}
			far_slice.plan = std::move(bound.plan);
			slice_binders.push_back(std::move(binder));
			slices.push_back(std::move(far_slice));
		}
		auto root = MakeFarRoot(*planner->binder, link, frame.streams, std::move(slices), failure);
		result = SubmitCrossingPlan(context, std::move(root));
		result->ThrowIfError();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		result.reset();
		failure->CloseHeld(error);
		for (auto slice : frame.slices) {
			for (idx_t k = 0; k < frame.streams; k++) {
				link.Open(CrossingSliceStreamId(slice, false, k))->Close(&error);
				link.Open(CrossingSliceStreamId(slice, true, k))->Close(&error);
			}
		}
		return end_transaction(error);
	}

	SeamPump pump(link, seams);
	ErrorData run_error;
	std::thread runner([&]() {
		try {
			result->Complete();
			result->ThrowIfError();
		} catch (std::exception &ex) {
			run_error = ErrorData(ex);
		}
		result.reset();
		{
			lock_guard<mutex> guard(lock);
			if (cancelled.HasError()) {
				run_error = cancelled;
			}
		}
		failure->CloseHeld(run_error);
		pump.StatementEnded();
	});
	pump.Run();
	runner.join();
	pump.Close(run_error);
	seams.clear();
	planner.reset();
	{
		lock_guard<mutex> guard(lock);
		running = nullptr;
		inboxes.clear();
	}
	return end_transaction(run_error);
}

void CrossingFar::RunDdl(CrossingStream &control, const string &frame) {
	try {
		auto &context = *connection.context;
		const string view_prefix(CROSSING_CONTROL_VIEW);
		unique_ptr<SQLStatement> statement;
		if (frame.compare(0, view_prefix.size(), view_prefix) == 0) {
			auto lines = StringUtil::Split(frame.substr(view_prefix.size()), '\n');
			auto json_at = frame.find('\n', view_prefix.size());
			for (idx_t l = 0; l < 3; l++) {
				json_at = frame.find('\n', json_at + 1);
			}
			auto create = make_uniq<CreateViewInfo>(
			    QualifiedName(Identifier(catalog), Identifier(lines[0]), Identifier(lines[1])));
			create->on_conflict = static_cast<OnCreateConflict>(std::stoul(lines[2]));
			create->query = DecodeSubstraitView(connection, catalog, frame.substr(json_at + 1),
			                                    StringUtil::Split(lines[3], ','), options.catalogs);
			auto created = make_uniq<CreateStatement>();
			created->info = std::move(create);
			statement = std::move(created);
		} else {
			Parser parser(context);
			parser.ParseQuery(frame);
			statement = std::move(parser.statements[0]);
			switch (statement->type) {
			case StatementType::CREATE_STATEMENT:
				statement->Cast<CreateStatement>().info->SetCatalog(Identifier(catalog));
				break;
			case StatementType::ALTER_STATEMENT: {
				auto &info = *statement->Cast<AlterStatement>().info;
				auto name = info.GetQualifiedName();
				info.SetQualifiedName(Identifier(catalog), name.Schema(), name.Name());
				break;
			}
			case StatementType::DROP_STATEMENT:
				statement->Cast<DropStatement>().info->SetCatalog(Identifier(catalog));
				break;
			default:
				throw InvalidInputException("crossing: far was sent a DDL frame that is not DDL");
			}
		}
		context.Query(std::move(statement), QueryParameters())->ThrowIfError();
	} catch (std::exception &ex) {
		SendControl(control, ControlError(ErrorData(ex)));
		return;
	}
	SendControl(control, CROSSING_CONTROL_DONE);
}

void CrossingFar::AnswerTables(CrossingStream &control, const string &schema) {
	try {
		auto prepared = connection.Prepare(
		    "SELECT table_name FROM duckdb_tables() WHERE database_name = ? AND schema_name = ? UNION ALL "
		    "SELECT view_name FROM duckdb_views() WHERE database_name = ? AND schema_name = ? AND NOT internal "
		    "ORDER BY 1");
		vector<Value> args {Value(catalog), Value(schema), Value(catalog), Value(schema)};
		auto result = prepared->Execute(args);
		result->ThrowIfError();
		string reply;
		while (auto chunk = result->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				reply += (reply.empty() ? "" : "\n") + chunk->GetValue(0, r).ToString();
			}
		}
		SendControl(control, reply);
	} catch (IOException &) {
		throw;
	} catch (std::exception &ex) {
		SendControl(control, ControlError(ErrorData(ex)));
	}
}

void CrossingFar::AnswerDescribe(CrossingStream &control, const string &question) {
	try {
		auto parts = StringUtil::Split(question, '\n');
		auto prepared = connection.Prepare(
		    "SELECT count(*) FROM duckdb_views() WHERE database_name = ? AND schema_name = ? AND view_name = ?");
		vector<Value> args {Value(catalog), Value(parts[0]), Value(parts[1])};
		auto counted = prepared->Execute(args);
		counted->ThrowIfError();
		bool view = counted->Fetch()->GetValue(0, 0).GetValue<int64_t>() > 0;
		auto result =
		    connection.Query("SELECT * FROM " + SQLIdentifier::ToString(catalog) + "." +
		                     SQLIdentifier::ToString(parts[0]) + "." + SQLIdentifier::ToString(parts[1]) + " LIMIT 0");
		result->ThrowIfError();
		string reply = view ? "view" : "table";
		reply += "\n" + to_string(result->ColumnCount());
		for (idx_t c = 0; c < result->ColumnCount(); c++) {
			reply += "\n" + result->GetNames()[c].GetIdentifierName() + "\n" + result->GetTypes()[c].ToString();
		}
		auto constraints = connection.Prepare(
		    "SELECT constraint_type, constraint_column_names FROM duckdb_constraints() WHERE database_name = ? AND "
		    "schema_name = ? AND table_name = ? AND constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'NOT NULL')");
		auto declared = constraints->Execute(args);
		declared->ThrowIfError();
		while (auto chunk = declared->Fetch()) {
			for (idx_t r = 0; r < chunk->size(); r++) {
				auto columns = ListValue::GetChildren(chunk->GetValue(1, r));
				reply += "\n" + chunk->GetValue(0, r).ToString() + "\n" + to_string(columns.size());
				for (auto &column : columns) {
					reply += "\n" + column.ToString();
				}
			}
		}
		SendControl(control, reply);
	} catch (IOException &) {
		throw;
	} catch (std::exception &ex) {
		SendControl(control, ControlError(ErrorData(ex)));
	}
}

bool CrossingFar::NextControl(CrossingStream &control, string &header) {
	while (true) {
		CrossingFrame frame;
		auto signal = make_shared_ptr<InterruptDoneSignalState>();
		auto parking = CrossingParking::Of(InterruptState(signal));
		{
			lock_guard<mutex> guard(lock);
			if (cancelled.HasError()) {
				return false;
			}
			control_wait = parking;
		}
		switch (control.Recv(frame, parking->Waker())) {
		case CrossingIo::ROWS:
			header = std::move(frame.header);
			return true;
		case CrossingIo::DONE:
			return false;
		default:
			if (parking->Park()) {
				signal->Await();
			}
		}
	}
}

void CrossingFar::Serve(CrossingLink &link) {
	auto control = link.Open(0);
	auto begin = connection.Query("BEGIN");
	if (begin->HasError()) {
		ErrorData why;
		{
			lock_guard<mutex> guard(lock);
			why = cancelled;
		}
		if (!why.HasError()) {
			begin->ThrowError();
		}
		control->Close(&why);
		return;
	}
	const string plan_prefix(CROSSING_CONTROL_PLAN);
	const string ddl_prefix(CROSSING_CONTROL_DDL);
	const string tables_prefix(CROSSING_CONTROL_TABLES);
	const string describe_prefix(CROSSING_CONTROL_DESCRIBE);
	string header;
	while (NextControl(*control, header)) {
		try {
			if (header.compare(0, plan_prefix.size(), plan_prefix) == 0) {
				if (RunStatement(link, *control, header.substr(plan_prefix.size()))) {
					return;
				}
				continue;
			}
			if (header.compare(0, ddl_prefix.size(), ddl_prefix) == 0) {
				RunDdl(*control, header.substr(ddl_prefix.size()));
				continue;
			}
			if (header.compare(0, tables_prefix.size(), tables_prefix) == 0) {
				AnswerTables(*control, header.substr(tables_prefix.size()));
				continue;
			}
			if (header.compare(0, describe_prefix.size(), describe_prefix) == 0) {
				AnswerDescribe(*control, header.substr(describe_prefix.size()));
				continue;
			}
		} catch (IOException &) {
			break;
		}
		auto end = connection.Query(header == CROSSING_CONTROL_COMMIT ? "COMMIT" : "ROLLBACK");
		if (end->HasError()) {
			SendControl(*control, ControlError(end->GetErrorObject()));
		} else {
			SendControl(*control, CROSSING_CONTROL_DONE);
		}
		control->Close(nullptr);
		return;
	}
	auto rollback = connection.Query("ROLLBACK");
	ErrorData why;
	{
		lock_guard<mutex> guard(lock);
		why = cancelled;
	}
	control->Close(why.HasError() ? &why : nullptr);
}

} // namespace duckdb
