#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

TEST_CASE("a read fragment arrives on the far side as the plan that was sent", "[transport]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Same("SELECT o.id, upper(c.cname) FROM far.orders o JOIN far.customers c ON o.tenant = c.tenant WHERE "
	          "o.amt > 100 ORDER BY o.id LIMIT 2");

	auto &call = twin.LastRead();
	REQUIRE(!call.wire.empty());
	REQUIRE(call.received_text == call.plan_text);
}

TEST_CASE("a write fragment carrying rows arrives with the rows", "[transport]") {
	Twin twin(Transport::NATIVE);
	twin.store->key_unique["orders"] = false;
	twin.Seed();

	twin.Query("UPDATE far.orders SET amt = 0 WHERE amt > 100");

	auto &call = twin.LastWrite();
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("a substrait read names the source's tables and nothing of the target's", "[transport]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	twin.Same("SELECT o.id FROM far.orders o JOIN far.customers c ON o.tenant = c.tenant WHERE o.amt > 100 ORDER BY "
	          "o.id");

	auto &call = twin.LastRead();
	REQUIRE(call.wire.find("\"namedTable\":{\"names\":[\"main\",\"orders\"]}") != string::npos);
	REQUIRE(call.wire.find("\"namedTable\":{\"names\":[\"main\",\"customers\"]}") != string::npos);
	REQUIRE(call.wire.find("far") == string::npos);
	REQUIRE(call.wire.find("crossing_floor") == string::npos);
}

TEST_CASE("a substrait write fed by a read of the same source renders as that read", "[transport]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	twin.Query("UPDATE far.orders SET amt = 0 WHERE id = 2");

	auto &call = twin.LastWrite();
	REQUIRE(call.wire.find("\"namedTable\":{\"names\":[\"main\",\"orders\"]}") != string::npos);
	REQUIRE(call.rows == vector<vector<Value>> {{Value::INTEGER(2), Value::INTEGER(0)}});
}

TEST_CASE("what the source cannot render is never offered to it", "[transport]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE amt > 100 AND (SELECT count(*) FROM far.customers) > 2 ORDER BY id");
	twin.Same("SELECT id FROM far.orders GROUP BY GROUPING SETS ((id), ()) ORDER BY id NULLS LAST");
	twin.Same("SELECT id FROM far.orders o WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant = o.tenant) "
	          "ORDER BY id");
	twin.Same("SELECT id, name FROM far.orders WHERE name SIMILAR TO 'a.*' ORDER BY id");
	twin.Same("SELECT id FROM far.orders ORDER BY id LIMIT 50 PERCENT");
}
