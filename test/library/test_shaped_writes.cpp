#include "framework/twin.hpp"

using namespace duckdb;

namespace {

Value Scalar(Twin &twin, const string &sql) {
	auto result = twin.store->con.Query(sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return result->Fetch()->GetValue(0, 0);
}

struct NoKeyTable {
	explicit NoKeyTable(idx_t rows = 5) : twin(Transport::LINK) {
		twin.Far("CREATE TABLE plain(a INTEGER, b INTEGER)");
		twin.Far("INSERT INTO plain SELECT i::INTEGER, 0 FROM range(" + to_string(rows) + ") r(i)");
		twin.Far("CREATE TABLE other(a INTEGER, c INTEGER)");
		twin.Far("INSERT INTO other VALUES (1, 11), (2, 22)");
		twin.Attach();
	}
	Twin twin;
};

} // namespace

TEST_CASE("an update with no key runs on far and sends near no rows", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("UPDATE far.plain SET b = a + 1 WHERE a > 2");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->link_seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(9));
}

TEST_CASE("a delete with no key runs on far and sends near no rows", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("DELETE FROM far.plain WHERE a < 2");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->link_seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain") == Value::BIGINT(3));
}

TEST_CASE("an update from another far table crosses whole", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("UPDATE far.plain SET b = o.c FROM far.other o WHERE plain.a = o.a");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->link_seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(33));
}

TEST_CASE("an update from near rows sends only those rows", "[shaped]") {
	NoKeyTable t;
	t.twin.Query("CREATE TABLE here(a INTEGER, v INTEGER)");
	t.twin.Query("INSERT INTO here VALUES (0, 5), (4, 6)");
	auto result = t.twin.Query("UPDATE far.plain SET b = h.v FROM here h WHERE plain.a = h.a");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->link_seam_rows == 2);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(11));
}

TEST_CASE("a merge reading far runs on far as one operator", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("MERGE INTO far.plain t USING far.other s ON t.a = s.a "
	                           "WHEN MATCHED THEN UPDATE SET b = s.c");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->link_seam_rows == 0);
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(33));
}

TEST_CASE("a merge from near rows sends only those rows, and all its actions land", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("MERGE INTO far.plain t USING (VALUES (0, 9), (9, 9)) s(a, c) ON t.a = s.a "
	                           "WHEN MATCHED THEN UPDATE SET b = s.c "
	                           "WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.c)");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(t.twin.store->link_seam_rows == 2);
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain") == Value::BIGINT(6));
	REQUIRE(Scalar(t.twin, "SELECT sum(b) FROM plain") == Value::HUGEINT(18));
}

TEST_CASE("a merge returns its rows and which action made them", "[shaped]") {
	NoKeyTable t;
	auto result = t.twin.Query("MERGE INTO far.plain t USING far.other s ON t.a = s.a "
	                           "WHEN MATCHED THEN UPDATE SET b = s.c RETURNING a, b, merge_action");
	REQUIRE(result->RowCount() == 2);
	auto first = result->GetValue(0, 0).GetValue<int32_t>();
	REQUIRE(result->GetValue(1, 0) == Value::INTEGER(first * 11));
	REQUIRE(result->GetValue(2, 1).ToString() == "UPDATE");
}

TEST_CASE("a table scanned while written in one statement reads as it was", "[shaped]") {
	NoKeyTable t(1000);
	auto result =
	    t.twin.Query("UPDATE far.plain SET a = a + 1 FROM (SELECT max(a) AS m FROM far.plain) x WHERE plain.a < x.m");
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(999));
	REQUIRE(Scalar(t.twin, "SELECT max(a) FROM plain") == Value::INTEGER(999));
	REQUIRE(Scalar(t.twin, "SELECT count(*) FROM plain WHERE a = 999") == Value::BIGINT(2));
}
