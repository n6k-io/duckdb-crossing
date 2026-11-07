#include "catch.hpp"
#include "crossing.hpp"
#include "framework/test_link.hpp"
#include "internal/execute.hpp"
#include "internal/far_plan.hpp"
#include "internal/parking.hpp"

#include "duckdb.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/planner.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <future>
#include <thread>

using namespace duckdb;

namespace {

constexpr idx_t FAR_TIMEOUT_SECONDS = 60;

struct FarDb {
	explicit FarDb(idx_t threads) : db(nullptr), con(db) {
		RegisterCrossingFarFunctions(*db.instance);
		REQUIRE_NO_FAIL(con.Query("SET threads = " + to_string(threads)));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i::BIGINT AS a FROM range(10000) r(i)"));
	}

	template <class RESULT>
	static void REQUIRE_NO_FAIL(RESULT result) {
		if (result->HasError()) {
			result->ThrowError();
		}
	}

	shared_ptr<CrossingSeamInbox> Seam(idx_t id, idx_t streams, bool ordered = false) {
		auto inbox = make_shared_ptr<CrossingSeamInbox>(*con.context, vector<LogicalType> {LogicalType::BIGINT},
		                                                ordered, streams);
		CrossingFarSeams::Of(*con.context).inboxes[id] = inbox;
		return inbox;
	}

	//! Runs the slices as one plan, on its own thread as a far server would.
	std::future<ErrorData> Start(CrossingLink &link, vector<std::pair<idx_t, string>> slices, bool ordered = false) {
		return std::async(std::launch::async, [this, &link, slices, ordered]() {
			try {
				REQUIRE_NO_FAIL(con.Query("BEGIN"));
				Planner planner(*con.context);
				vector<CrossingFarSlice> far_slices;
				for (auto &slice : slices) {
					Parser parser(*con.context);
					parser.ParseQuery(slice.second);
					auto &select = parser.statements[0]->Cast<SelectStatement>();
					auto bound = planner.binder->Bind(*select.node);
					CrossingFarSlice far_slice;
					far_slice.id = slice.first;
					far_slice.ordered = ordered;
					far_slice.plan = std::move(bound.plan);
					far_slices.push_back(std::move(far_slice));
				}
				auto root = MakeFarRoot(*planner.binder, link, link.Streams(), std::move(far_slices),
				                        make_shared_ptr<CrossingFailure>());
				auto result = SubmitCrossingPlan(*con.context, std::move(root));
				result->ThrowIfError();
				result->Complete();
				result->ThrowIfError();
				result.reset();
				REQUIRE_NO_FAIL(con.Query("COMMIT"));
				return ErrorData();
			} catch (std::exception &ex) {
				return ErrorData(ex);
			}
		});
	}

	DuckDB db;
	Connection con;
};

//! Stands where near would be: drains one slice's output.
struct DrainReceiver : public CrossingReceiver {
	const vector<LogicalType> &Types() const override {
		static const vector<LogicalType> types {LogicalType::BIGINT};
		return types;
	}
	void Deliver(CrossingFrame &frame) override {
		std::lock_guard<std::mutex> guard(lock);
		for (idx_t r = 0; r < frame.rows.size(); r++) {
			rows.push_back(frame.rows.GetValue(0, r).GetValue<int64_t>());
		}
	}
	void Closed(optional_ptr<const ErrorData>) override {
		closed++;
		ended.notify_all();
	}

	std::mutex lock;
	std::condition_variable ended;
	std::atomic<idx_t> closed {0};
	vector<int64_t> rows;
};

vector<int64_t> Drain(CrossingLink &link, idx_t slice) {
	DrainReceiver receiver;
	vector<unique_ptr<CrossingStream>> streams;
	for (idx_t k = 0; k < link.Streams(); k++) {
		streams.push_back(link.Open(CrossingSliceStreamId(slice, false, k), &receiver));
	}
	{
		std::unique_lock<std::mutex> guard(receiver.lock);
		while (receiver.closed < streams.size()) {
			receiver.ended.wait_for(guard, std::chrono::milliseconds(10));
		}
	}
	for (auto &stream : streams) {
		stream->Close(nullptr);
	}
	return receiver.rows;
}

void RequireFinishes(std::future<ErrorData> &far) {
	REQUIRE(far.wait_for(std::chrono::seconds(FAR_TIMEOUT_SECONDS)) == std::future_status::ready);
	auto error = far.get();
	if (error.HasError()) {
		FAIL(error.Message());
	}
}

idx_t next_sequence = 0;

void AppendRange(CrossingSeamInbox &inbox, int64_t from, int64_t to, idx_t batch = 0) {
	auto append = [&](DataChunk &rows) {
		CrossingRowHeader header;
		header.sequence = next_sequence++;
		header.batch = batch;
		header.min_batch = 0;
		inbox.Append(rows, header);
	};
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	idx_t r = 0;
	for (int64_t v = from; v < to; v++) {
		chunk.SetValue(0, r++, Value::BIGINT(v));
		if (r == STANDARD_VECTOR_SIZE) {
			chunk.SetChildCardinality(r);
			append(chunk);
			chunk.Reset();
			r = 0;
		}
	}
	if (r > 0) {
		chunk.SetChildCardinality(r);
		append(chunk);
	}
}

} // namespace

TEST_CASE("a far plan with no seam streams its rows out", "[far]") {
	auto threads = GENERATE(idx_t(1), idx_t(4));
	INFO(threads << " far threads");
	FarDb far(threads);
	TestLinkPair link(2);

	auto running = far.Start(link.Far(), {{0, "SELECT a FROM t"}});
	auto rows = Drain(link.Near(), 0);

	RequireFinishes(running);
	REQUIRE(rows.size() == 10000);
}

TEST_CASE("a seam parks until its rows arrive, under a filter and a join", "[far]") {
	auto threads = GENERATE(idx_t(1), idx_t(4));
	INFO(threads << " far threads");
	FarDb far(threads);
	TestLinkPair link(1);
	auto seam = far.Seam(7, 1);

	auto running = far.Start(link.Far(), {{0, "SELECT t.a FROM crossing_seam_rows(7::UBIGINT) s JOIN t ON s.c0 = t.a "
	                                          "WHERE s.c0 % 2 = 0"}});
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(running.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout);

	AppendRange(*seam, 0, 5000);
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	AppendRange(*seam, 5000, 6000);
	seam->StreamDone();

	auto rows = Drain(link.Near(), 0);
	RequireFinishes(running);
	REQUIRE(rows.size() == 3000);
}

TEST_CASE("one far plan runs a slice that waits on another slice's rows", "[far]") {
	auto threads = GENERATE(idx_t(1), idx_t(4));
	INFO(threads << " far threads");
	FarDb far(threads);
	TestLinkPair link(1);
	auto seam = far.Seam(9, 1);

	auto running = far.Start(
	    link.Far(), {{0, "SELECT a FROM t WHERE a < 3000"}, {1, "SELECT c0 + 1 FROM crossing_seam_rows(9::UBIGINT)"}});

	auto first = Drain(link.Near(), 0);
	AppendRange(*seam, 0, NumericCast<int64_t>(first.size()));
	seam->StreamDone();
	auto second = Drain(link.Near(), 1);

	RequireFinishes(running);
	REQUIRE(first.size() == 3000);
	REQUIRE(second.size() == 3000);
}

TEST_CASE("a seam releases batches in order across streams", "[far]") {
	next_sequence = 0;
	auto threads = GENERATE(idx_t(1), idx_t(4));
	INFO(threads << " far threads");
	FarDb far(threads);
	TestLinkPair link(1);
	auto seam = far.Seam(3, 2, true);

	auto running =
	    far.Start(link.Far(), {{0, "SELECT c0 FROM crossing_seam_ordered_rows(3::UBIGINT)"}}, /* ordered */ true);

	AppendRange(*seam, 4000, 8000, 2);
	AppendRange(*seam, 0, 4000, 1);
	seam->StreamDone();
	seam->StreamDone();

	auto rows = Drain(link.Near(), 0);
	RequireFinishes(running);
	REQUIRE(rows.size() == 8000);
	for (idx_t r = 0; r < rows.size(); r++) {
		if (rows[r] != NumericCast<int64_t>(r)) {
			FAIL("row " << r << " out of order: " << rows[r]);
		}
	}
}

TEST_CASE("a cancelled seam fails the far plan with its error", "[far]") {
	FarDb far(2);
	TestLinkPair link(1);
	auto seam = far.Seam(5, 1);

	auto running = far.Start(link.Far(), {{0, "SELECT c0 FROM crossing_seam_rows(5::UBIGINT)"}});
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	seam->Cancel(ErrorData(ExceptionType::CONSTRAINT, "the feed failed"));

	REQUIRE(running.wait_for(std::chrono::seconds(FAR_TIMEOUT_SECONDS)) == std::future_status::ready);
	auto error = running.get();
	REQUIRE(error.HasError());
	REQUIRE(error.Type() == ExceptionType::CONSTRAINT);
}

TEST_CASE("far's optimizer sees near's estimate of a seam", "[far]") {
	FarDb far(1);
	auto seam = far.Seam(11, 1);
	seam->estimate = 123456;

	auto explained = far.con.Query("EXPLAIN SELECT * FROM crossing_seam_rows(11::UBIGINT)");
	if (explained->HasError()) {
		FAIL(explained->GetError());
	}
	REQUIRE(explained->ToString().find("123,456") != string::npos);
}
