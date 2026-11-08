#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

bool HasJoin(const FarCall &call) {
	return call.Has(Op::LOGICAL_COMPARISON_JOIN) || call.Has(Op::LOGICAL_ANY_JOIN) ||
	       call.Has(Op::LOGICAL_CROSS_PRODUCT);
}

void Seed(Twin &twin) {
	twin.store->takes_seams = true;
	twin.Seed();
	twin.Both("CREATE TABLE loc(id INTEGER, x INTEGER)");
	twin.Both("INSERT INTO loc VALUES (1, 10), (2, 20), (3, 30), (99, 40)");
	twin.Both("CREATE TABLE lookup(x INTEGER, y INTEGER)");
	twin.Both("INSERT INTO lookup VALUES (10, 100), (20, 200)");
}

Value Scalar(Twin &twin, const string &far_sql) {
	auto result = twin.store->con.Query(far_sql);
	if (result->HasError()) {
		FAIL(result->GetError());
	}
	return Collected(std::move(result))->GetValue(0, 0);
}

} // namespace

TEST_CASE("a join to rows the target holds runs on the source, with those rows sent", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT o.id, o.name, l.x FROM far.orders o JOIN loc l ON o.id = l.id ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(HasJoin(call));
	REQUIRE(call.Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(call.rows.size() == 3);
}

TEST_CASE("work above a cut join runs on the source too", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT o.tenant, sum(l.x) AS s FROM far.orders o JOIN loc l ON o.id = l.id GROUP BY o.tenant ORDER BY "
	          "o.tenant");

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(HasJoin(twin.LastRead()));
}

TEST_CASE("without the source's opt in a join to the target's rows stays near", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.store->takes_seams = false;

	twin.Same("SELECT o.id, l.x FROM far.orders o JOIN loc l ON o.id = l.id ORDER BY o.id");

	REQUIRE(!HasJoin(twin.LastRead()));
}

TEST_CASE("a source refusing a seam column's type keeps the join near", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.Both("CREATE TABLE typed(id INTEGER, d DOUBLE)");
	twin.Both("INSERT INTO typed VALUES (1, 1.5)");
	twin.store->refused_types.push_back(LogicalTypeId::DOUBLE);

	twin.Same("SELECT o.id, t.d FROM far.orders o JOIN typed t ON o.id = t.id");

	REQUIRE(!HasJoin(twin.LastRead()));
}

TEST_CASE("joins that stay near still answer the same", "[seams]") {
	auto sql = GENERATE(
	    as<string> {}, "SELECT o.id, l.x FROM far.orders o LEFT JOIN loc l ON o.id = l.id ORDER BY o.id",
	    "SELECT o.id, l.x FROM far.orders o RIGHT JOIN loc l ON o.id = l.id ORDER BY l.id",
	    "SELECT o.id FROM far.orders o WHERE o.id IN (SELECT id FROM loc) ORDER BY o.id",
	    "SELECT o.id FROM far.orders o WHERE o.id NOT IN (SELECT id FROM loc) ORDER BY o.id",
	    "SELECT o.id FROM far.orders o SEMI JOIN loc l ON o.id = l.id ORDER BY o.id",
	    "SELECT o.id FROM far.orders o ANTI JOIN loc l ON o.id = l.id ORDER BY o.id",
	    "SELECT o.id, l.x FROM far.orders o, loc l ORDER BY o.id, l.x",
	    "SELECT o.id, l.x, k.y FROM far.orders o, loc l, lookup k WHERE o.id = k.x / 10 AND l.x = k.x ORDER BY o.id");
	INFO(sql);
	Twin twin;
	Seed(twin);

	twin.Same(sql);

	REQUIRE(!HasJoin(twin.LastRead()));
}

TEST_CASE("a condition the source cannot compute keeps the join near", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.store->refused_functions.insert("least");

	twin.Same("SELECT o.id FROM far.orders o, loc l WHERE least(o.id, l.id) = 3 ORDER BY o.id, l.id");

	REQUIRE(!HasJoin(twin.LastRead()));
}

TEST_CASE("only the first join crosses, and later rows join near", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same(
	    "SELECT o.id, l.x, k.y FROM far.orders o JOIN loc l ON o.id = l.id JOIN lookup k ON l.x = k.x ORDER BY o.id");
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(HasJoin(twin.LastRead()));
	REQUIRE(twin.LastRead().rows.size() == 3);

	twin.Same("SELECT o.id, l.x, k.y FROM far.orders o, loc l, lookup k WHERE o.id = l.id AND l.x = k.x ORDER BY o.id");
	REQUIRE(HasJoin(twin.LastRead()));
	REQUIRE(twin.LastRead().rows.size() == 3);
}

TEST_CASE("a join of local rows goes over as one seam", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT o.id, j.y FROM far.orders o JOIN (SELECT l.id, k.y FROM loc l JOIN lookup k ON l.x = k.x) j "
	          "ON o.id = j.id ORDER BY o.id");

	REQUIRE(HasJoin(twin.LastRead()));
	REQUIRE(twin.LastRead().rows.size() == 2);
}

TEST_CASE("a filter on the local side leaves only matching rows to send", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT o.id FROM far.orders o JOIN loc l ON o.id = l.id WHERE l.x > 15 ORDER BY o.id");

	REQUIRE(HasJoin(twin.LastRead()));
	REQUIRE(!twin.LastRead().Mentions("> 15"));
	REQUIRE(twin.LastRead().rows.size() == 2);
}

TEST_CASE("a filter on the source's side runs there", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT o.id FROM far.orders o JOIN loc l ON o.id = l.id WHERE o.amt > 100 ORDER BY o.id");

	REQUIRE(HasJoin(twin.LastRead()));
	REQUIRE(twin.LastRead().Mentions("amt >"));
}

TEST_CASE("two seams never share one crossing", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT id FROM (SELECT o.id FROM far.orders o JOIN loc l ON o.id = l.id "
	          "UNION ALL SELECT o.id FROM far.archive o JOIN lookup k ON o.id = k.x / 10 + 8) ORDER BY id");

	REQUIRE(twin.store->reads.size() == 2);
	for (auto &call : twin.store->reads) {
		REQUIRE(call.Count(Op::LOGICAL_CHUNK_GET) == 1);
	}
}

TEST_CASE("an empty seam still makes the call and answers as the query would", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.Both("CREATE TABLE none(id INTEGER)");
	twin.Far("CREATE TABLE counted(c BIGINT)");

	twin.Same("SELECT count(*) FROM far.orders o JOIN none n ON o.id = n.id");
	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(HasJoin(twin.LastRead()));
	twin.Same("SELECT o.id FROM far.orders o JOIN none n ON o.id = n.id");

	twin.Query("INSERT INTO far.counted SELECT count(*) FROM far.orders o JOIN none n ON o.id = n.id");
	REQUIRE(twin.store->writes.size() == 1);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM counted") == Value::BIGINT(1));
	REQUIRE(Scalar(twin, "SELECT c FROM counted") == Value::BIGINT(0));
}

TEST_CASE("an update from local rows is one plan on the source", "[seams]") {
	Twin twin;
	Seed(twin);

	auto result = twin.Query("UPDATE far.orders o SET amt = l.x FROM loc l WHERE o.id = l.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(twin.store->reads.empty());
	REQUIRE(twin.store->writes.size() == 1);
	REQUIRE(twin.LastWrite().Has(Op::LOGICAL_CHUNK_GET));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 2") == Value::INTEGER(20));
	REQUIRE(Scalar(twin, "SELECT amt FROM orders WHERE id = 4") == Value::INTEGER(350));
}

TEST_CASE("an update from local rows that select nothing changes nothing and reports zero", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.Both("CREATE TABLE none(id INTEGER, x INTEGER)");

	auto result = twin.Query("UPDATE far.orders o SET amt = n.x FROM none n WHERE o.id = n.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(0));
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders WHERE amt = 50") == Value::BIGINT(1));
}

TEST_CASE("a delete and an insert over a local join run on the source", "[seams]") {
	Twin twin;
	Seed(twin);

	auto inserted = twin.Query("INSERT INTO far.orders SELECT o.id + 100, o.name, l.x, o.tenant, o.score, o.ts "
	                           "FROM far.orders o JOIN loc l ON o.id = l.id");
	REQUIRE(inserted->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(HasJoin(twin.LastWrite()));
	REQUIRE(twin.store->reads.empty());

	auto deleted = twin.Query("DELETE FROM far.orders USING loc l WHERE orders.id = l.id RETURNING id");
	REQUIRE(deleted->RowCount() == 3);
	REQUIRE(Scalar(twin, "SELECT count(*) FROM orders") == Value::BIGINT(5));
}

TEST_CASE("a CTE on either side keeps the join near", "[seams]") {
	auto sql = GENERATE(as<string> {},
	                    "WITH c AS MATERIALIZED (SELECT id, x FROM loc) SELECT o.id, c.x, d.x FROM far.orders o "
	                    "JOIN c ON o.id = c.id JOIN c d ON o.id = d.id ORDER BY o.id",
	                    "WITH c AS MATERIALIZED (SELECT id FROM far.orders) SELECT c.id, l.x FROM c "
	                    "JOIN loc l ON c.id = l.id JOIN c d ON c.id = d.id ORDER BY c.id");
	INFO(sql);
	Twin twin;
	Seed(twin);

	twin.Same(sql);
}

TEST_CASE("a seam over the memory limit spills and every row joins", "[seams]") {
	Twin twin;
	twin.store->takes_seams = true;
	twin.Far("CREATE TABLE big(id BIGINT)");
	twin.Far("INSERT INTO big SELECT i FROM range(32000) r(i)");
	twin.Attach();
	twin.Query("CREATE TABLE near_rows AS SELECT i AS id, repeat(md5(i::VARCHAR), 32) AS v FROM range(32000) r(i)");
	twin.Query("SET memory_limit = '16MB'");

	auto result = twin.Query("SELECT count(*), sum(strlen(n.v)) FROM far.big g JOIN near_rows n ON g.id = n.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(32000));
	REQUIRE(HasJoin(twin.LastRead()));
}

TEST_CASE("a seam rows' order and duplicates reach the join", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.Both("CREATE TABLE twice(id INTEGER, tag VARCHAR)");
	twin.Both("INSERT INTO twice VALUES (1, 'a'), (1, 'b'), (NULL, 'c')");

	twin.Same("SELECT o.id, t.tag FROM far.orders o JOIN twice t ON o.id = t.id ORDER BY t.tag");
	REQUIRE(HasJoin(twin.LastRead()));
	twin.Same("SELECT t.tag, o.id FROM twice t JOIN far.orders o ON t.id = o.id ORDER BY t.tag");
	REQUIRE(HasJoin(twin.LastRead()));
}

TEST_CASE("a prepared join sends the rows each time", "[seams]") {
	Twin twin;
	Seed(twin);

	auto prepared = twin.con.Prepare("SELECT count(*) FROM far.orders o JOIN loc l ON o.id = l.id WHERE l.x > ?");
	REQUIRE(!prepared->HasError());
	REQUIRE(Collected(prepared->Execute(0))->GetValue(0, 0) == Value::BIGINT(3));
	REQUIRE(Collected(prepared->Execute(15))->GetValue(0, 0) == Value::BIGINT(2));
}

TEST_CASE("a view over local rows is still refused", "[seams]") {
	Twin twin;
	Seed(twin);

	auto error = twin.Error("CREATE VIEW far.v AS SELECT o.id FROM far.orders o JOIN memory.main.loc l ON o.id = l.id");

	REQUIRE_THAT(error, Catch::Contains("does not run wholly on the source"));
}

TEST_CASE("a merge from local rows gives the same rows with seams on", "[seams]") {
	auto sql = GENERATE(as<string> {},
	                    "MERGE INTO far.orders t USING loc s ON t.id = s.id WHEN MATCHED THEN UPDATE SET amt = s.x",
	                    "MERGE INTO far.orders t USING loc s ON t.id = s.id WHEN MATCHED THEN DELETE",
	                    "MERGE INTO far.orders t USING loc s ON t.id = s.id WHEN MATCHED THEN UPDATE SET amt = s.x "
	                    "WHEN NOT MATCHED THEN INSERT (id, amt) VALUES (s.id, s.x)");
	INFO(sql);
	const string rows = "SELECT string_agg(o::VARCHAR, ';' ORDER BY id) FROM orders o";

	Twin plain;
	Seed(plain);
	plain.Far(Twin::Unprefixed(sql));

	Twin twin;
	Seed(twin);
	twin.Query(sql);

	REQUIRE(Scalar(twin, rows) == Scalar(plain, rows));
}

TEST_CASE("subqueries over local rows give the same rows with seams on", "[seams]") {
	auto sql =
	    GENERATE(as<string> {},
	             "SELECT o.id, (SELECT max(l.x) FROM loc l WHERE l.id = o.id) AS m FROM far.orders o ORDER BY o.id",
	             "SELECT o.id FROM far.orders o WHERE EXISTS (SELECT 1 FROM loc l WHERE l.id = o.id) ORDER BY o.id",
	             "SELECT l.id, (SELECT max(o.amt) FROM far.orders o JOIN loc m ON o.id = m.id WHERE m.id = l.id) AS m "
	             "FROM loc l ORDER BY l.id");
	INFO(sql);
	Twin twin;
	Seed(twin);

	twin.Same(sql);
}

TEST_CASE("a table created from a cut join holds the join's rows", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Query("CREATE TABLE far.made AS SELECT o.id, l.x FROM far.orders o JOIN loc l ON o.id = l.id");

	REQUIRE(Scalar(twin, "SELECT count(*) FROM made") == Value::BIGINT(3));
	REQUIRE(Scalar(twin, "SELECT x FROM made WHERE id = 2") == Value::INTEGER(20));
}

TEST_CASE("a join with another source's rows stays near", "[seams]") {
	Twin twin;
	Seed(twin);
	twin.Attach("other.example.com", "other");

	auto result = twin.Query("SELECT o.id, c.cname FROM far.orders o JOIN other.customers c ON o.tenant = c.tenant");

	REQUIRE(result->RowCount() == 4);
	REQUIRE(twin.store->reads.size() == 2);
	for (auto &call : twin.store->reads) {
		REQUIRE(!HasJoin(call));
		REQUIRE(!call.Has(Op::LOGICAL_CHUNK_GET));
	}
}

TEST_CASE("a cut join reads the local columns only the query above it needs", "[seams]") {
	Twin twin;
	Seed(twin);

	twin.Same("SELECT l.x FROM far.orders o JOIN loc l ON o.id = l.id WHERE o.id = 2");
	twin.Same("SELECT o.name FROM far.orders o JOIN loc l ON o.id = l.id ORDER BY o.name NULLS LAST LIMIT 2");
	twin.Same("SELECT count(*) FROM far.orders o JOIN loc l ON o.id = l.id");
	REQUIRE(twin.store->reads.size() == 3);
}

TEST_CASE("the recursive part of a recursive query cannot read a source's table", "[seams]") {
	Twin twin;
	Seed(twin);

	REQUIRE_THAT(twin.Error("WITH RECURSIVE r(id) AS (SELECT 1 UNION ALL SELECT r.id + 1 FROM r JOIN far.orders o ON "
	                        "r.id = o.id JOIN loc l ON o.id = l.id) SELECT id FROM r ORDER BY id"),
	             Catch::Contains("WITH RECURSIVE"));
}
