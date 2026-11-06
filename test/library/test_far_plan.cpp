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

#include <chrono>
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
				auto root =
				    MakeFarRoot(*planner.binder, link, std::move(far_slices), make_shared_ptr<CrossingFailure>());
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
vector<int64_t> Drain(CrossingLink &link, idx_t slice) {
	vector<int64_t> out;
	vector<unique_ptr<CrossingStream>> streams;
	for (idx_t k = 0; k < link.Streams(); k++) {
		streams.push_back(link.Open(CrossingSliceStreamId(slice, false, k)));
	}
	auto signal = make_shared_ptr<InterruptDoneSignalState>();
	vector<bool> done(streams.size(), false);
	idx_t open = streams.size();
	while (open > 0) {
		signal = make_shared_ptr<InterruptDoneSignalState>();
		auto parking = CrossingParking::Of(InterruptState(signal));
		bool progressed = false;
		for (idx_t k = 0; k < streams.size(); k++) {
			if (done[k]) {
				continue;
			}
			CrossingFrame frame;
			frame.rows.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
			auto io = streams[k]->Recv(frame, parking->Waker());
			if (io == CrossingIo::ROWS) {
				progressed = true;
				for (idx_t r = 0; r < frame.rows.size(); r++) {
					out.push_back(frame.rows.GetValue(0, r).GetValue<int64_t>());
				}
			} else if (io == CrossingIo::DONE) {
				progressed = true;
				done[k] = true;
				open--;
			}
		}
		if (!progressed && parking->Park()) {
			signal->Await();
		}
	}
	return out;
}

void RequireFinishes(std::future<ErrorData> &far) {
	REQUIRE(far.wait_for(std::chrono::seconds(FAR_TIMEOUT_SECONDS)) == std::future_status::ready);
	auto error = far.get();
	if (error.HasError()) {
		FAIL(error.Message());
	}
}

void AppendRange(CrossingSeamInbox &inbox, idx_t stream, int64_t from, int64_t to, idx_t batch = 0) {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::BIGINT});
	idx_t r = 0;
	for (int64_t v = from; v < to; v++) {
		chunk.SetValue(0, r++, Value::BIGINT(v));
		if (r == STANDARD_VECTOR_SIZE) {
			chunk.SetChildCardinality(r);
			inbox.Append(stream, chunk, batch);
			chunk.Reset();
			r = 0;
		}
	}
	if (r > 0) {
		chunk.SetChildCardinality(r);
		inbox.Append(stream, chunk, batch);
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

	AppendRange(*seam, 0, 0, 5000);
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	AppendRange(*seam, 0, 5000, 6000);
	seam->StreamDone(0);

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
	AppendRange(*seam, 0, 0, NumericCast<int64_t>(first.size()));
	seam->StreamDone(0);
	auto second = Drain(link.Near(), 1);

	RequireFinishes(running);
	REQUIRE(first.size() == 3000);
	REQUIRE(second.size() == 3000);
}

TEST_CASE("a seam releases batches in order across streams", "[far]") {
	auto threads = GENERATE(idx_t(1), idx_t(4));
	INFO(threads << " far threads");
	FarDb far(threads);
	TestLinkPair link(1);
	auto seam = far.Seam(3, 2, true);

	auto running =
	    far.Start(link.Far(), {{0, "SELECT c0 FROM crossing_seam_ordered_rows(3::UBIGINT)"}}, /* ordered */ true);

	AppendRange(*seam, 1, 4000, 8000, 2);
	AppendRange(*seam, 0, 0, 4000, 1);
	seam->StreamDone(0);
	seam->StreamDone(1);

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
