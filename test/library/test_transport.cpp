#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

string WireForAllColumns(Twin &twin) {
	twin.Far("CREATE TABLE t(a INTEGER, b INTEGER, c INTEGER)");
	twin.Far("INSERT INTO t VALUES (1, 2, 3), (4, 5, 6)");
	twin.Attach();
	twin.Query("SELECT a, b, c FROM far.t");
	REQUIRE(twin.LastRead().wire.find("baseSchema") != string::npos);
	return twin.LastRead().wire;
}

string DecodeError(Twin &twin, const string &wire) {
	auto &con = twin.store->con;
	auto catalog = Collected(con.Query("SELECT current_database()"))->GetValue(0, 0).ToString();
	try {
		auto result = DecodeSubstraitJson(con, catalog, wire)->Execute();
		return result->HasError() ? result->GetError() : "";
	} catch (std::exception &ex) {
		return ex.what();
	}
}

} // namespace

TEST_CASE("a substrait read decodes against the table it was planned for", "[transport][stale_schema]") {
	Twin twin(Transport::SUBSTRAIT);
	auto wire = WireForAllColumns(twin);

	REQUIRE(DecodeError(twin, wire) == "");
}

TEST_CASE("a substrait read planned before a column was dropped reports the change", "[transport][stale_schema]") {
	Twin twin(Transport::SUBSTRAIT);
	auto wire = WireForAllColumns(twin);

	twin.Far("ALTER TABLE t DROP COLUMN b");

	REQUIRE_THAT(DecodeError(twin, wire), Catch::Contains("changed on the source"));
}

TEST_CASE("a substrait read planned before any other change to the table reports the change",
          "[transport][stale_schema]") {
	Twin twin(Transport::SUBSTRAIT);
	auto wire = WireForAllColumns(twin);

	SECTION("a column renamed") {
		twin.Far("ALTER TABLE t RENAME COLUMN b TO z");
	}
	SECTION("a column retyped under its own name") {
		twin.Far("ALTER TABLE t ALTER COLUMN b TYPE BIGINT");
	}
	SECTION("a column added at the end") {
		twin.Far("ALTER TABLE t ADD COLUMN d INTEGER");
	}

	REQUIRE_THAT(DecodeError(twin, wire), Catch::Contains("changed on the source"));
}

TEST_CASE("a read through an attach that has not seen a change reports the change", "[transport][stale_schema]") {
	Twin twin(Transport::SUBSTRAIT);
	WireForAllColumns(twin);

	twin.Far("ALTER TABLE t DROP COLUMN b");
	twin.Far("ALTER TABLE t ADD COLUMN d INTEGER");

	REQUIRE_THAT(twin.Error("SELECT a, b, c FROM far.t"), Catch::Contains("changed on the source"));
}

TEST_CASE("a substrait read planned before a column was replaced is not remapped by position",
          "[transport][stale_schema]") {
	Twin twin(Transport::SUBSTRAIT);
	auto wire = WireForAllColumns(twin);

	twin.Far("ALTER TABLE t DROP COLUMN b");
	SECTION("by a new name") {
		twin.Far("ALTER TABLE t ADD COLUMN d INTEGER");
	}
	SECTION("by the same name, now last") {
		twin.Far("ALTER TABLE t ADD COLUMN b INTEGER");
	}

	REQUIRE_THAT(DecodeError(twin, wire), Catch::Contains("changed on the source"));
}

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

TEST_CASE("a substrait write joining a VALUES list to a read holds the rows in the plan", "[transport]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	twin.Query("UPDATE far.orders SET name = v.name FROM (VALUES (2, 'changed')) v(id, name) WHERE orders.id = v.id");

	auto &call = twin.LastWrite();
	REQUIRE(!call.rows_only);
	REQUIRE(call.wire.find("\"virtualTable\":{\"expressions\":[{") != string::npos);
	REQUIRE(call.wire.find("\"namedTable\":{\"names\":[\"main\",\"orders\"]}") != string::npos);
	REQUIRE(call.rows.size() == 1);
}

TEST_CASE("a substrait write of gathered rows leaves them out of the JSON", "[transport]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	twin.Query("INSERT INTO far.orders VALUES (6, 'zeta', 600, 3, 6.5, NULL)");

	auto &call = twin.LastWrite();
	REQUIRE(call.rows_only);
	REQUIRE(call.wire.empty());
	REQUIRE(call.rows.size() == 1);
	twin.Same("SELECT * FROM far.orders ORDER BY id");
}

TEST_CASE("a substrait plan holding rows decodes from the JSON alone", "[transport][rows]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	twin.Same("SELECT o.id FROM far.orders o JOIN (VALUES (1), (2)) t(x) ON o.id = t.x ORDER BY o.id");

	auto wire = twin.LastRead().wire;
	REQUIRE(wire.find("\"virtualTable\":{\"expressions\":[{") != string::npos);
	REQUIRE(wire.find("seam_") == string::npos);
	REQUIRE(DecodeError(twin, wire) == "");
}

TEST_CASE("a substrait plan holding no rows decodes with its column types", "[transport][rows]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();
	twin.Query("SELECT t.x FROM far.orders o JOIN (VALUES (1, 7)) t(k, x) ON o.id = t.k");
	auto wire = twin.LastRead().wire;
	auto one_row = string("{\"fields\":[{\"literal\":{\"i32\":1}},{\"literal\":{\"i32\":7}}]}");
	REQUIRE(wire.find(one_row) != string::npos);

	auto &con = twin.store->con;
	auto catalog = Collected(con.Query("SELECT current_database()"))->GetValue(0, 0).ToString();
	auto result = Collected(DecodeSubstraitJson(con, catalog, StringUtil::Replace(wire, one_row, ""))->Execute());

	REQUIRE(!result->HasError());
	REQUIRE(result->RowCount() == 0);
	REQUIRE(result->types == vector<LogicalType> {LogicalType::INTEGER});
}

TEST_CASE("a value in a VALUES list arrives as itself", "[transport][rows]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	auto value = GENERATE(as<string> {}, "NULL::INTEGER", "true", "1::TINYINT", "2::SMALLINT", "3", "4::BIGINT",
	                      "5::HUGEINT", "1.5::FLOAT", "2.5::DOUBLE", "'nan'::FLOAT", "'inf'::FLOAT", "'nan'::DOUBLE",
	                      "'inf'::DOUBLE", "'-inf'::DOUBLE", "12.50::DECIMAL(10,2)", "'it''s'", "'a' || chr(0) || 'z'",
	                      "'\\x00\\xFF'::BLOB", "DATE '2024-01-01'", "TIME '12:34:56.789'",
	                      "TIMESTAMP '2024-01-01 12:34:56.789'", "TIMESTAMPTZ '2024-01-01 12:34:56+00'",
	                      "INTERVAL 1 DAY", "'00000000-0000-0000-0000-000000000001'::UUID", "[1, NULL, 3]",
	                      "{'a': 1, 'z': 'it''s'}", "[{'a': [1]}]", "MAP {'k': 1}");
	INFO(value);

	twin.Same("SELECT t.x FROM far.orders o JOIN (VALUES (1, " + value + ")) t(k, x) ON o.id = t.k");

	REQUIRE(twin.LastRead().wire.find("\"virtualTable\":{\"expressions\":[{") != string::npos);
}

TEST_CASE("a substrait read keeps the field names of a struct_pack", "[transport][struct_pack]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();

	SECTION("a field read by name") {
		twin.Same("SELECT struct_pack(a := id, b := name).b AS v FROM far.orders ORDER BY id");
		REQUIRE(twin.LastRead().wire.find("struct_pack") != string::npos);
	}
	SECTION("the whole struct") {
		twin.Same("SELECT struct_pack(a := id, b := name) AS v FROM far.orders ORDER BY id");
		REQUIRE(twin.LastRead().wire.find("struct_pack") != string::npos);
	}
	SECTION("a name that needs quoting, read by name") {
		twin.Same("SELECT struct_pack(\"Mixed Case\" := id, \"select\" := name).\"Mixed Case\" AS v FROM far.orders "
		          "ORDER BY id");
		REQUIRE(twin.LastRead().wire.find("struct_pack") != string::npos);
	}
	SECTION("a name that needs quoting, in the whole struct") {
		twin.Same("SELECT struct_pack(\"Mixed Case\" := id, \"select\" := name) AS v FROM far.orders ORDER BY id");
		REQUIRE(twin.LastRead().wire.find("struct_pack") != string::npos);
	}
	SECTION("a struct inside a struct, read by name") {
		twin.Same("SELECT struct_pack(a := struct_pack(x := id, y := name)).a.y AS v FROM far.orders ORDER BY id");
		REQUIRE(twin.LastRead().wire.find("struct_pack") != string::npos);
	}
	SECTION("a struct inside a struct, whole") {
		twin.Same("SELECT struct_pack(a := struct_pack(x := id, y := name)) AS v FROM far.orders ORDER BY id");
		REQUIRE(twin.LastRead().wire.find("struct_pack") != string::npos);
	}
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
