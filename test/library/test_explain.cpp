#include "framework/twin.hpp"

using namespace duckdb;

namespace {

string Explained(Twin &twin, const string &sql) {
	auto result = twin.Query("EXPLAIN " + sql);
	string text;
	for (idx_t r = 0; r < result->RowCount(); r++) {
		text += result->GetValue(1, r).ToString() + "\n";
	}
	return text;
}

struct Explaining {
	Explaining() {
		twin.Far("CREATE TABLE a(i BIGINT PRIMARY KEY, k BIGINT)");
		twin.Far("INSERT INTO a SELECT range, range % 100 FROM range(100)");
		twin.Attach();
		twin.Query("CREATE TABLE here(i BIGINT, v BIGINT)");
		twin.Query("INSERT INTO here VALUES (1, 10), (2, 20)");
	}
	Twin twin;
};

} // namespace

TEST_CASE("EXPLAIN of a read shows the plan the source runs", "[explain]") {
	Explaining e;
	auto text = Explained(e.twin, "SELECT k FROM far.a WHERE i < 10");
	INFO(text);
	REQUIRE_THAT(text, Catch::Contains("Runs on source"));
	REQUIRE_THAT(text, Catch::Contains("i < "));
	REQUIRE_THAT(text, !Catch::Contains("Sends"));
}

TEST_CASE("EXPLAIN of a write shows the write the source runs and what it returns", "[explain]") {
	Explaining e;
	auto text = Explained(e.twin, "UPDATE far.a SET k = 1 WHERE i < 10");
	INFO(text);
	REQUIRE_THAT(text, Catch::Contains("Runs on source"));
	REQUIRE_THAT(text, Catch::Contains("UPDATE main.a"));
	REQUIRE_THAT(text, Catch::Contains("i < "));
	REQUIRE_THAT(text, Catch::Contains("Returns: count"));
	REQUIRE_THAT(text, !Catch::Contains("Sends"));
}

TEST_CASE("EXPLAIN of a write fed from local rows shows what it sends", "[explain]") {
	Explaining e;
	auto text = Explained(e.twin, "UPDATE far.a SET k = h.v FROM here h WHERE a.i = h.i");
	INFO(text);
	REQUIRE_THAT(text, Catch::Contains("Runs on source"));
	REQUIRE_THAT(text, Catch::Contains("UPDATE main.a"));
	auto sends = text.find("Sends");
	REQUIRE(sends != string::npos);
	REQUIRE_THAT(text.substr(sends, text.find('\n', sends) - sends), Catch::Contains("~2 rows"));
}
