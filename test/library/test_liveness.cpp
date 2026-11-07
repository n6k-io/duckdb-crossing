#include "framework/twin.hpp"

#include "duckdb/main/query_result_stream.hpp"

#include <chrono>
#include <future>
#include <thread>

using namespace duckdb;

namespace {

constexpr int STUCK_SECONDS = 15;
constexpr int INTERRUPT_SECONDS = 5;

//! Runs `sql`; stuck says it did not end in time. Releases the link either way, so the statement can end and the test
//! fail instead of hanging.
struct Running {
	Running(Twin &twin_p, const string &sql)
	    : twin(twin_p), result(std::async(std::launch::async, [this, sql]() { return twin.con.Query(sql); })) {
	}

	bool EndsWithin(int seconds) {
		auto ended = result.wait_for(std::chrono::seconds(seconds)) == std::future_status::ready;
		twin.store->Release();
		outcome = result.get();
		return ended;
	}

	Twin &twin;
	std::future<unique_ptr<QueryResult>> result;
	unique_ptr<QueryResult> outcome;
};

void Interrupt(Twin &twin) {
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	twin.con.Interrupt();
}

void SeedBig(Twin &twin, idx_t streams, idx_t capacity, idx_t rows) {
	twin.store->streams = streams;
	twin.store->capacity = capacity;
	twin.Far("SET threads = 4");
	twin.Far("CREATE TABLE big AS SELECT i::BIGINT AS id, 0::BIGINT AS v FROM range(" + to_string(rows) + ") r(i)");
	twin.Attach();
}

} // namespace

TEST_CASE("a read ends when near has fewer tasks than far has streams", "[liveness]") {
	auto preserve = GENERATE(true, false);
	auto near_threads = GENERATE(idx_t(1), idx_t(2));
	INFO("preserve order " << preserve << ", " << near_threads << " near threads");
	Twin twin;
	SeedBig(twin, 4, 1, 3000000);
	twin.Query("SET threads = " + to_string(near_threads));
	twin.Query(string("SET preserve_insertion_order = ") + (preserve ? "true" : "false"));

	Running read(twin, "SELECT id FROM far.big");
	REQUIRE(read.EndsWithin(STUCK_SECONDS));
	REQUIRE(!read.outcome->HasError());
	REQUIRE(read.outcome->Collection().Count() == 3000000);
}

TEST_CASE("a read uses several streams", "[liveness]") {
	Twin twin;
	SeedBig(twin, 4, 4, 3000000);
	twin.Query("SET threads = 4");
	twin.Query("SET preserve_insertion_order = false");

	Running read(twin, "SELECT id FROM far.big");
	REQUIRE(read.EndsWithin(STUCK_SECONDS));
	REQUIRE(!read.outcome->HasError());
	REQUIRE(read.outcome->Collection().Count() == 3000000);
	read.outcome.reset();

	REQUIRE(twin.store->near_recv_streams > 1);
}

TEST_CASE("an interrupt ends a read waiting on far", "[liveness]") {
	auto near_threads = GENERATE(idx_t(1), idx_t(4));
	INFO(near_threads << " near threads");
	Twin twin;
	SeedBig(twin, 1, 4, 1000);
	twin.Query("SET threads = " + to_string(near_threads));
	twin.store->stall_results = true;

	Running read(twin, "SELECT id FROM far.big");
	Interrupt(twin);
	REQUIRE(read.EndsWithin(INTERRUPT_SECONDS));
	REQUIRE_THAT(read.outcome->GetError(), Catch::Contains("Interrupted"));
}

TEST_CASE("an interrupt ends a merge without RETURNING waiting on far", "[liveness]") {
	auto near_threads = GENERATE(idx_t(1), idx_t(4));
	INFO(near_threads << " near threads");
	Twin twin;
	SeedBig(twin, 1, 4, 1000);
	twin.Query("SET threads = " + to_string(near_threads));
	twin.store->stall_results = true;

	Running merge(twin, "MERGE INTO far.big t USING (SELECT 1::BIGINT AS id) s ON t.id = s.id "
	                    "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 1");
	Interrupt(twin);
	REQUIRE(merge.EndsWithin(INTERRUPT_SECONDS));
	REQUIRE_THAT(merge.outcome->GetError(), Catch::Contains("Interrupted"));
}

TEST_CASE("an interrupt ends DDL waiting on far", "[liveness]") {
	Twin twin;
	SeedBig(twin, 1, 4, 10);
	twin.Query("BEGIN");
	twin.Query("SELECT count(*) FROM far.big");
	twin.store->stall_replies = true;

	Running ddl(twin, "CREATE TABLE far.main.n(a INTEGER)");
	Interrupt(twin);
	REQUIRE(ddl.EndsWithin(INTERRUPT_SECONDS));
	REQUIRE_THAT(ddl.outcome->GetError(), Catch::Contains("Interrupted"));
}

TEST_CASE("far holds back what near has not read", "[liveness]") {
	auto preserve = GENERATE(true, false);
	INFO("preserve order " << preserve);
	Twin twin;
	SeedBig(twin, 4, 1000000, 6000000);
	twin.Query("SET threads = 2");
	twin.Query(string("SET preserve_insertion_order = ") + (preserve ? "true" : "false"));

	QueryResultStream<ChunkFormat> stream(twin.con.Submit("SELECT id FROM far.big"));
	auto first = stream.Fetch();
	REQUIRE(first);
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	REQUIRE(twin.store->far_rows_sent.load() < 3000000);

	idx_t rows = first->size();
	while (auto chunk = stream.Fetch()) {
		rows += chunk->size();
	}
	REQUIRE(rows == 6000000);
	REQUIRE(twin.store->far_rows_sent.load() == 6000000);
}

TEST_CASE("a transport that delivers from one thread carries every direction at once", "[liveness]") {
	Twin twin;
	SeedBig(twin, 4, 1, 1000000);
	twin.Query("SET threads = 2");
	twin.Query("SET preserve_insertion_order = false");
	twin.Query("CREATE TABLE here AS SELECT i::BIGINT AS id FROM range(500000) r(i)");
	twin.store->takes_seams = true;

	Running both(twin, "SELECT count(*) FROM (SELECT b.id FROM far.big b JOIN here h ON b.id = h.id OFFSET 0) x");
	REQUIRE(both.EndsWithin(STUCK_SECONDS));
	REQUIRE(!both.outcome->HasError());
}
