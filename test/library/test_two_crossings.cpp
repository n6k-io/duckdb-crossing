#include "framework/twin.hpp"

using namespace duckdb;

TEST_CASE("two crossings on one instance each read only their own source", "[two_crossings]") {
	auto transport = EACH_TRANSPORT();
	auto other_first = GENERATE(false, true);
	INFO(TransportName(transport) << (other_first ? " other first" : " far first"));
	Pair pair(transport, other_first);
	pair.Seed();

	REQUIRE(pair.Query("SELECT count(*) FROM far.t")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(pair.far->reads.size() == 1);
	REQUIRE(pair.other->reads.empty());

	REQUIRE(pair.Query("SELECT count(*) FROM other.t")->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(pair.far->reads.size() == 1);
	REQUIRE(pair.other->reads.size() == 1);
}

TEST_CASE("two crossings on one instance each write only their own source", "[two_crossings]") {
	auto transport = EACH_TRANSPORT();
	auto other_first = GENERATE(false, true);
	INFO(TransportName(transport) << (other_first ? " other first" : " far first"));
	Pair pair(transport, other_first);
	pair.Seed();

	pair.Query("INSERT INTO far.t VALUES (9, 90)");
	pair.Query("UPDATE other.t SET v = 0 WHERE id = 1");
	pair.Query("DELETE FROM far.t WHERE id = 1");

	REQUIRE(pair.far->writes.size() == 2);
	REQUIRE(pair.other->writes.size() == 1);
	REQUIRE(Pair::Scalar(*pair.far, "SELECT count(*) FROM t") == Value::BIGINT(2));
	REQUIRE(Pair::Scalar(*pair.far, "SELECT v FROM t WHERE id = 9") == Value::INTEGER(90));
	REQUIRE(Pair::Scalar(*pair.other, "SELECT count(*) FROM t") == Value::BIGINT(3));
	REQUIRE(Pair::Scalar(*pair.other, "SELECT v FROM t WHERE id = 1") == Value::INTEGER(0));
}
