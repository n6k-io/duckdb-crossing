#include "framework/twin.hpp"

using namespace duckdb;

TEST_CASE("a partitioned reader is drained on every partition and every row arrives once", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->read_partitions = 3;
	twin.Seed();

	twin.Same("SELECT id FROM far.orders ORDER BY id");

	REQUIRE(twin.store->partitions_read == set<idx_t> {0, 1, 2});
	twin.Same("SELECT count(DISTINCT id), count(*) FROM far.orders");
}

TEST_CASE("a partitioned reader feeds a keyed write", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->read_partitions = 2;
	twin.store->key_unique["orders"] = false;
	twin.Seed();

	auto result = twin.Query("UPDATE far.orders SET amt = amt + 1 WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(4));
	twin.Same("SELECT amt FROM far.orders ORDER BY id");
	REQUIRE(twin.store->con.Query("SELECT amt FROM orders WHERE id = 2")->GetValue(0, 0) == Value::INTEGER(151));
}

TEST_CASE("every reader opened is released when the statement is done", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->read_partitions = 3;
	twin.Seed();

	twin.Query("SELECT id FROM far.orders");
	twin.Query("SELECT id FROM far.orders LIMIT 1");

	REQUIRE(twin.store->readers_open >= 2);
	REQUIRE(twin.store->readers_closed == twin.store->readers_open);
}

TEST_CASE("a reader that waits is asked again once it is woken", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_chunks = true;
	twin.Seed();

	twin.Same("SELECT id FROM far.orders ORDER BY id");

	REQUIRE(twin.store->waits_served == 2);
}

TEST_CASE("a wake that arrives before the pull parks is not lost", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_chunks = true;
	twin.store->wake_before_returning = true;
	twin.Seed();

	twin.Same("SELECT id FROM far.orders ORDER BY id");

	REQUIRE(twin.store->waits_served == 2);
}

TEST_CASE("a limit over parked partitioned readers returns, and late wakes are harmless", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_chunks = true;
	twin.store->keep_wakers = true;
	twin.store->read_partitions = 3;
	twin.Seed();

	auto result = twin.Query("SELECT id FROM far.orders LIMIT 1");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(!twin.store->kept_wakers.empty());
	for (auto &waker : twin.store->kept_wakers) {
		waker.Wake();
	}
	twin.Same("SELECT count(*) FROM far.orders");
}

TEST_CASE("a partitioned reader that waits is drained on every partition", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_chunks = true;
	twin.store->read_partitions = 3;
	twin.Seed();

	twin.Same("SELECT count(*) FROM far.orders");

	REQUIRE(twin.store->partitions_read == set<idx_t> {0, 1, 2});
	REQUIRE(twin.store->waits_served == 6);
}

TEST_CASE("a writer that waits is asked again once it is woken", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_write = true;
	twin.Seed();

	auto result = twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.store->write_waits_served == 1);
	twin.Same("SELECT count(*) FROM far.orders");
}

TEST_CASE("a write run wholly on the source waits the same way", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_write = true;
	twin.Seed();

	auto result = twin.Query("DELETE FROM far.orders WHERE amt > 100");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(4));
	REQUIRE(twin.store->write_waits_served == 1);
	REQUIRE(twin.store->con.Query("SELECT count(*) FROM orders")->GetValue(0, 0) == Value::BIGINT(1));
}

TEST_CASE("a merge whose write waits finishes", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.store->wait_before_write = true;
	twin.Seed();

	auto result = twin.Query("MERGE INTO far.orders t USING (SELECT id, amt FROM far.archive) s ON t.id = s.id "
	                         "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 'm', s.amt, 0, NULL, NULL)");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(1));
	REQUIRE(twin.store->write_waits_served == 1);
}

TEST_CASE("a source that throws while reading fails the statement with its message", "[protocol]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();
	twin.Far("DROP TABLE archive");

	auto error = twin.Error("SELECT count(*) FROM far.archive");

	REQUIRE_THAT(error, Catch::Contains("archive"));
	twin.Same("SELECT count(*) FROM far.orders");
}
