#include "crossing_far.hpp"

#include "internal/control_frames.hpp"
#include "internal/far_plan.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/query_result.hpp"
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

void RequireOk(unique_ptr<QueryResult> result) {
	result->ThrowIfError();
	result->Complete();
	result->ThrowIfError();
}

} // namespace

CrossingFar::CrossingFar(Connection &connection_p, string catalog_p, CrossingFarOptions options_p)
    : connection(connection_p), catalog(std::move(catalog_p)), options(std::move(options_p)) {
	RegisterCrossingFarFunctions(*connection.context->db);
}

void CrossingFar::RunStatement(CrossingLink &link, CrossingStream &control, const string &plan) {
	auto &context = *connection.context;
	auto failure = make_shared_ptr<CrossingFailure>();
	auto &seams = CrossingFarSeams::Of(context).inboxes;
	seams.clear();

	unique_ptr<Planner> planner;
	vector<shared_ptr<Binder>> slice_binders;
	unique_ptr<QueryResult> result;
	try {
		auto decoded = DecodeSubstraitSlices(connection, catalog, plan, options.catalogs, options.floors);
		for (auto &seam : decoded.seams) {
			auto inbox = make_shared_ptr<CrossingSeamInbox>(context, seam.types, seam.ordered, link.Streams());
			inbox->estimate = seam.estimate;
			seams[seam.id] = std::move(inbox);
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
		auto root = MakeFarRoot(*planner->binder, link, std::move(slices), failure);
		result = SubmitCrossingPlan(context, std::move(root));
		result->ThrowIfError();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		result.reset();
		failure->CloseHeld(error);
		SendControl(control, ControlError(error));
		return;
	}
	SendControl(control, CROSSING_CONTROL_BOUND);

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
		failure->CloseHeld(run_error);
		pump.StatementEnded();
	});
	pump.Run();
	runner.join();
	pump.Close(run_error);
	seams.clear();
	planner.reset();
	SendControl(control, run_error.HasError() ? ControlError(run_error) : string(CROSSING_CONTROL_DONE));
}

void CrossingFar::Serve(CrossingLink &link) {
	auto control = link.Open(0);
	if (!options.transaction_open) {
		auto begin = connection.Query("BEGIN");
		begin->ThrowIfError();
	}
	const string plan_prefix(CROSSING_CONTROL_PLAN);
	string header;
	while (RecvControl(*control, header)) {
		if (header.compare(0, plan_prefix.size(), plan_prefix) == 0) {
			try {
				RunStatement(link, *control, header.substr(plan_prefix.size()));
			} catch (IOException &) {
				break;
			}
			continue;
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
	control->Close(nullptr);
}

} // namespace duckdb
