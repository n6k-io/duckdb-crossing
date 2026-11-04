#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

constexpr const char *PLAIN_FORMS = "SELECT id FROM far.orders WHERE amt > 100 AND amt BETWEEN 100 AND 400 AND "
                                    "CAST(amt AS BIGINT) < 1000 ORDER BY id";

void SeedMixedCase(Twin &twin) {
	twin.Far("CREATE SCHEMA MixedSchema");
	twin.Far("CREATE TABLE MixedSchema.MixedCase(Id INTEGER PRIMARY KEY, CamelCol INTEGER)");
	twin.Far("INSERT INTO MixedSchema.MixedCase VALUES (1, 10), (2, 20)");
	twin.Attach();
}

} // namespace

TEST_CASE("a comparison, a between and a cast are never asked about as calls", "[engine_contract]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(PLAIN_FORMS);

	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	REQUIRE(twin.store->calls_asked.empty());
}

TEST_CASE("a cast or a between over a refused call stays on the target", "[engine_contract]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("length");
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE CAST(length(name) AS DOUBLE) > 4.5 ORDER BY id");
	REQUIRE(!twin.LastRead().Mentions("length"));
	REQUIRE(twin.LastRead().rows.size() == 5);

	twin.Same("SELECT id FROM far.orders WHERE length(name) BETWEEN 5 AND 6 ORDER BY id");
	REQUIRE(!twin.LastRead().Mentions("length"));
	REQUIRE(twin.LastRead().rows.size() == 5);
}

TEST_CASE("a substrait plan carries casts and comparisons in Substrait's own terms", "[engine_contract]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	twin.Same(PLAIN_FORMS);

	auto &wire = twin.LastRead().wire;
	INFO(wire);
	REQUIRE(wire.find("__") == string::npos);
	REQUIRE(wire.find("\"cast\"") != string::npos);
	REQUIRE(wire.find("\"gt") != string::npos);
}

TEST_CASE("a native plan of comparisons, betweens and casts arrives as it was sent", "[engine_contract]") {
	Twin twin(Transport::NATIVE);
	twin.Seed();

	twin.Same(PLAIN_FORMS);

	auto &call = twin.LastRead();
	REQUIRE(call.received_text == call.plan_text);
}

TEST_CASE("a read names the source's tables as the source spells them", "[engine_contract]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	SeedMixedCase(twin);

	twin.Same("SELECT * FROM far.MIXEDSCHEMA.MixedCase WHERE CAMELCOL > 10 ORDER BY ID",
	          "SELECT * FROM MixedSchema.MixedCase WHERE CamelCol > 10 ORDER BY Id");

	auto &call = twin.LastRead();
	REQUIRE(call.tables.size() == 1);
	REQUIRE(call.tables[0].schema == "MixedSchema");
	REQUIRE(call.tables[0].table == "MixedCase");
}

TEST_CASE("a write names the source's table and columns as the source spells them", "[engine_contract]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	SeedMixedCase(twin);

	twin.Query("UPDATE far.mixedschema.MixedCase SET CAMELCOL = 0 WHERE ID = 2");

	auto &call = twin.LastWrite();
	REQUIRE(call.written.schema == "MixedSchema");
	REQUIRE(call.written.table == "MixedCase");
	REQUIRE(call.key_columns == vector<string> {"Id"});
	REQUIRE(call.set_columns == vector<string> {"CamelCol"});
	REQUIRE(Pair::Scalar(*twin.store, "SELECT CamelCol FROM MixedSchema.MixedCase WHERE Id = 2") == Value::INTEGER(0));
}
