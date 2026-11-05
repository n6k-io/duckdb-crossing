#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

bool HasJoin(const FarCall &call) {
	return call.Has(Op::LOGICAL_COMPARISON_JOIN) || call.Has(Op::LOGICAL_ANY_JOIN) ||
	       call.Has(Op::LOGICAL_CROSS_PRODUCT);
}

bool HasLimit(const FarCall &call) {
	return call.Has(Op::LOGICAL_LIMIT) || call.Has(Op::LOGICAL_TOP_N);
}

} // namespace

TEST_CASE("a filter and an order the source can compute run on the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT name FROM far.orders WHERE amt > 100 ORDER BY id");

	auto &call = twin.LastRead();
	REQUIRE(call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.Has(Op::LOGICAL_ORDER_BY));
	REQUIRE(call.rows.size() == 4);
	REQUIRE(call.types == vector<LogicalType> {LogicalType::VARCHAR});
}

TEST_CASE("the source is read for the columns the query needs, in the order it names them", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT amt, id FROM far.orders ORDER BY id");

	auto &call = twin.LastRead();
	REQUIRE(call.types == vector<LogicalType> {LogicalType::INTEGER, LogicalType::INTEGER});
	REQUIRE(call.rows[0] == vector<Value> {Value::INTEGER(50), Value::INTEGER(1)});
}

TEST_CASE("a mixed filter leaves only the half the source cannot compute on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE amt > 100 AND upper(name) = 'BETA'");

	auto &call = twin.LastRead();
	REQUIRE(call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.Mentions("amt >"));
	REQUIRE(!call.Mentions("upper"));
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("a filter the source cannot compute at all stays on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE upper(name) = 'BETA'");

	auto &call = twin.LastRead();
	REQUIRE(!call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.rows.size() == 5);
}

TEST_CASE("a limit and an offset cross with the order under them", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders ORDER BY amt DESC LIMIT 2 OFFSET 1");

	auto &call = twin.LastRead();
	REQUIRE(HasLimit(call));
	REQUIRE(call.rows.size() == 2);
}

TEST_CASE("an order whose key the source cannot compute stays, and the source sends every row", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("lower");
	twin.Seed();

	twin.Same("SELECT id FROM far.orders ORDER BY lower(name) NULLS LAST LIMIT 2");

	auto &call = twin.LastRead();
	REQUIRE(!call.Has(Op::LOGICAL_ORDER_BY));
	REQUIRE(!HasLimit(call));
	REQUIRE(call.rows.size() == 5);
}

TEST_CASE("aggregates the source knows cross whole", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT tenant, sum(amt) FROM far.orders GROUP BY tenant ORDER BY tenant");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(twin.LastRead().rows.size() == 3);

	twin.Same("SELECT count(*) FROM far.orders");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(twin.LastRead().rows.size() == 1);

	twin.Same("SELECT tenant, count(DISTINCT name) FROM far.orders GROUP BY tenant ORDER BY tenant");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
}

TEST_CASE("an aggregate the source refuses stays, and only the scan crosses", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("sum");
	twin.Seed();

	twin.Same("SELECT tenant, sum(amt) FROM far.orders GROUP BY tenant ORDER BY tenant");

	auto &call = twin.LastRead();
	REQUIRE(!call.Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(call.rows.size() == 5);
}

TEST_CASE("a window the source knows crosses; one it refuses stays", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY amt DESC) FROM far.orders ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_WINDOW));

	twin.store->refused_functions.insert("row_number");
	twin.Same("SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY amt DESC) FROM far.orders ORDER BY id");
	REQUIRE(!twin.LastRead().Has(Op::LOGICAL_WINDOW));
}

TEST_CASE("a join of two tables of one source is one query on the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT o.id, c.cname FROM far.orders o JOIN far.customers c ON o.tenant = c.tenant ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(HasJoin(call));
	REQUIRE(call.tables.size() == 2);
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("an IN over one source's tables is one query on the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE tenant IN (SELECT tenant FROM far.customers WHERE cname <> 'acme') "
	          "ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(HasJoin(call));
	REQUIRE(call.tables.size() == 2);
	REQUIRE(call.rows.size() == 2);
}

TEST_CASE("an IN beside another condition is one query on the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE amt > 100 AND tenant IN (SELECT tenant FROM far.customers) ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(HasJoin(call));
	REQUIRE(call.tables.size() == 2);
	REQUIRE(call.rows.size() == 3);
}

TEST_CASE("an IN over a table the target holds keeps the join on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.Both("CREATE TABLE regions(tenant INTEGER)");
	twin.Both("INSERT INTO regions VALUES (1), (2)");

	twin.Same("SELECT id FROM far.orders WHERE tenant IN (SELECT tenant FROM regions) ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(!HasJoin(twin.LastRead()));
}

TEST_CASE("an IN whose NULL result matters answers as the source would", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto names = string("(SELECT name FROM far.orders WHERE id > 3)");
	twin.Same("SELECT id FROM far.orders WHERE name IN " + names + " ORDER BY id");
	twin.Same("SELECT id FROM far.orders WHERE name NOT IN " + names + " ORDER BY id");
	twin.Same("SELECT id, name IN " + names + " FROM far.orders ORDER BY id");
	twin.Same("SELECT id FROM far.orders WHERE (name IN " + names + ") IS NULL ORDER BY id");
	twin.Same("SELECT id FROM far.orders WHERE id = 1 OR name IN " + names + " ORDER BY id");

	REQUIRE(twin.store->reads.size() == 5);
}

TEST_CASE("two INs over one source's tables are one query on the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE tenant IN (SELECT tenant FROM far.customers) "
	          "AND id IN (SELECT tenant FROM far.customers WHERE cname <> 'globex') ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(call.tables.size() == 3);
	REQUIRE(call.rows.size() == 2);
}

TEST_CASE("an IN over several columns is one query on the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT o.id FROM far.orders o WHERE (o.tenant, o.amt) IN "
	          "(SELECT i.tenant, i.amt FROM far.orders i WHERE i.amt > 100) ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(call.tables.size() == 2);
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("a comparison with ANY or ALL over one source's tables is one query on the source", "[pushdown]") {
	auto sql = GENERATE(as<string> {},
	                    "SELECT id FROM far.orders WHERE amt > ANY (SELECT tenant FROM far.customers) "
	                    "ORDER BY id",
	                    "SELECT id FROM far.orders WHERE amt > ALL (SELECT tenant FROM far.customers) ORDER BY id",
	                    "SELECT id FROM far.orders WHERE tenant <> ANY (SELECT tenant FROM far.customers) ORDER BY id",
	                    "SELECT id FROM far.orders WHERE tenant = ALL (SELECT tenant FROM far.customers) ORDER BY id");
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(sql);

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.LastRead().tables.size() == 2);
}

TEST_CASE("an IN crosses when a filter beside it stays on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE upper(name) = 'BETA' AND tenant IN (SELECT tenant FROM far.customers)");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(HasJoin(call));
	REQUIRE(!call.Mentions("upper"));
	REQUIRE(call.tables.size() == 2);
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("two VALUES lists in one plan each read their own rows", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(
	    "SELECT id FROM far.orders WHERE tenant IN (VALUES (1), (2)) AND id IN (VALUES (2), (3), (5)) ORDER BY id");
	twin.Same("SELECT o.id FROM far.orders o JOIN (VALUES (1), (2)) t(x) ON o.tenant = t.x "
	          "JOIN (VALUES (2), (3), (5)) u(y) ON o.id = u.y ORDER BY o.id");
}

TEST_CASE("a NOT IN or an IN read as a value over one source's tables is one query on the source", "[pushdown]") {
	auto sql = GENERATE(
	    as<string> {},
	    "SELECT id FROM far.orders WHERE tenant NOT IN (SELECT tenant FROM far.customers WHERE cname <> 'acme') "
	    "ORDER BY id",
	    "SELECT id, tenant IN (SELECT tenant FROM far.customers WHERE cname <> 'acme') FROM far.orders ORDER BY id");
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(sql);

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.LastRead().tables.size() == 2);
}

TEST_CASE("a correlated subquery over one source's tables is one query on the source", "[pushdown]") {
	auto sql = GENERATE(
	    as<string> {},
	    "SELECT id FROM far.orders o WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant = o.tenant) ORDER BY "
	    "id",
	    "SELECT id FROM far.orders o WHERE NOT EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant = o.tenant) "
	    "ORDER BY id",
	    "SELECT id FROM far.orders o WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant > o.tenant) ORDER BY "
	    "id",
	    "SELECT id FROM far.orders o WHERE tenant IN (SELECT tenant FROM far.customers c WHERE c.tenant < o.amt) "
	    "ORDER BY id",
	    "SELECT id FROM far.orders o WHERE tenant NOT IN (SELECT tenant FROM far.customers c WHERE c.tenant < o.amt) "
	    "ORDER BY id",
	    "SELECT id, (SELECT cname FROM far.customers c WHERE c.tenant = o.tenant) FROM far.orders o ORDER BY id");
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(sql);

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.LastRead().tables.size() == 2);
}

TEST_CASE("a correlated NOT IN answers each outer row by its own group's NULLs", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders o WHERE name NOT IN (SELECT i.name FROM far.orders i WHERE i.amt < o.amt) "
	          "ORDER BY id");
	twin.Same("SELECT id FROM far.orders o WHERE (name IN (SELECT i.name FROM far.orders i WHERE i.amt < o.amt)) "
	          "IS NULL ORDER BY id");

	REQUIRE(twin.store->reads.size() == 2);
}

TEST_CASE("a correlated scalar matching several rows fails as the source would", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	REQUIRE_THAT(
	    twin.Error("SELECT tenant, (SELECT o.id FROM far.orders o WHERE o.tenant = c.tenant) FROM far.customers c"),
	    Catch::Contains("More than one row"));
}

TEST_CASE("a correlated scalar stays on a target that does not raise the multi-row error", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.Query("SET scalar_subquery_error_on_multiple_rows = false");

	auto result =
	    twin.Query("SELECT tenant, (SELECT o.id FROM far.orders o WHERE o.tenant = c.tenant) FROM far.customers c");

	REQUIRE(result->RowCount() == 3);
	REQUIRE(twin.store->reads.size() == 2);
}

TEST_CASE("a correlated subquery over a table the target holds keeps the join on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.Both("CREATE TABLE regions(tenant INTEGER)");
	twin.Both("INSERT INTO regions VALUES (2), (3)");

	twin.Same(
	    "SELECT id FROM far.orders o WHERE EXISTS (SELECT 1 FROM regions r WHERE r.tenant > o.tenant) ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(!HasJoin(twin.LastRead()));
}

TEST_CASE("a correlated subquery with a filter the source refuses reads each side apart", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	twin.Same("SELECT id FROM far.orders o WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant > o.tenant AND "
	          "upper(c.cname) <> 'ACME') ORDER BY id");

	REQUIRE(twin.store->reads.size() == 2);
	for (auto &read : twin.store->reads) {
		REQUIRE(!read.Mentions("upper"));
	}
}

TEST_CASE("a CTE over one source's tables is one query on the source", "[pushdown]") {
	auto sql =
	    GENERATE(as<string> {},
	             "WITH x AS MATERIALIZED (SELECT id, tenant FROM far.orders) "
	             "SELECT a.id FROM x a JOIN x e ON a.id = e.tenant ORDER BY a.id",
	             "WITH x AS MATERIALIZED (SELECT id FROM far.orders), y AS MATERIALIZED (SELECT id FROM x WHERE "
	             "id > 2) SELECT x.id FROM x JOIN y ON x.id = y.id ORDER BY x.id",
	             "WITH v(t) AS MATERIALIZED (VALUES (1), (2)) "
	             "SELECT o.id FROM far.orders o JOIN v a ON o.tenant = a.t JOIN v e ON o.id = e.t ORDER BY o.id");
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(sql);

	REQUIRE(twin.store->reads.size() == 1);
}

TEST_CASE("a CTE reference to a sample reads the same rows every time", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto result = twin.Query("WITH x AS MATERIALIZED (SELECT id FROM far.orders USING SAMPLE 2) "
	                         "SELECT count(*) FROM x a JOIN x e ON a.id = e.id");

	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(twin.store->reads.size() == 1);
}

TEST_CASE("an order inside a CTE's body comes back in order across partitions", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->read_partitions = 4;
	twin.Seed();

	twin.Same("WITH x AS MATERIALIZED (SELECT id FROM far.orders) SELECT a.id FROM x a JOIN x e ON a.id = e.id "
	          "ORDER BY a.id DESC");

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.LastRead().ordered);
}

TEST_CASE("a limit under a correlated subquery crosses whichever comparison it uses", "[pushdown]") {
	auto comparison = GENERATE(as<string> {}, "=", ">");
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport) << " " << comparison);
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM (SELECT * FROM far.orders ORDER BY id LIMIT 3) o WHERE EXISTS (SELECT 1 FROM "
	          "far.customers c WHERE c.tenant " +
	          comparison + " o.tenant) ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
}

TEST_CASE("stacked correlated subqueries read each table use once", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders o WHERE EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant > o.tenant) "
	          "AND EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant > o.amt) "
	          "AND EXISTS (SELECT 1 FROM far.customers c WHERE c.tenant > o.id) ORDER BY id");

	REQUIRE(twin.store->reads.size() == 1);
	idx_t orders = 0;
	for (auto &use : twin.LastRead().tables) {
		orders += use.table == "orders" ? 1 : 0;
	}
	REQUIRE(orders == 1);
}

TEST_CASE("a correlated count and a lateral are one query on the source", "[pushdown]") {
	auto sql = GENERATE(
	    as<string> {},
	    "SELECT id, (SELECT count(*) FROM far.customers c WHERE c.tenant > o.tenant) FROM far.orders o ORDER BY id",
	    "SELECT o.id, l.n FROM far.orders o, LATERAL (SELECT count(*) AS n FROM far.customers c WHERE c.tenant > "
	    "o.tenant) l ORDER BY o.id");
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same(sql);

	REQUIRE(twin.store->reads.size() == 1);
}

TEST_CASE("a join with a table the target holds keeps the join on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.Both("CREATE TABLE regions(tenant INTEGER, region VARCHAR)");
	twin.Both("INSERT INTO regions VALUES (1, 'north'), (2, 'south')");

	twin.Same("SELECT o.id, r.region FROM far.orders o JOIN regions r ON o.tenant = r.tenant WHERE o.amt > 100 "
	          "ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &call = twin.LastRead();
	REQUIRE(!HasJoin(call));
	REQUIRE(call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("two attaches of one source type are two sources that never share a query", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	twin.Attach("second", "b");

	twin.Same("SELECT o.id, c.cname FROM far.orders o JOIN b.customers c ON o.tenant = c.tenant ORDER BY o.id",
	          "SELECT o.id, c.cname FROM orders o JOIN customers c ON o.tenant = c.tenant ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 2);
	for (auto &call : twin.store->reads) {
		REQUIRE(!HasJoin(call));
		REQUIRE(call.tables.size() == 1);
	}
}

TEST_CASE("expressions the source knows cross inside filters and projections", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE name IS NULL OR score IS NULL ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT id FROM far.orders WHERE amt BETWEEN 100 AND 300 ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT id FROM far.orders WHERE id IN (1, 3, 5) ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT id FROM far.orders WHERE NOT (id IN (1, 3, 5)) ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT id FROM far.orders WHERE name LIKE '%a' ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT id FROM far.orders WHERE ts > TIMESTAMP '2024-01-02 12:00:00' ORDER BY id");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT id, CASE WHEN amt > 300 THEN 'big' WHEN amt > 100 THEN 'mid' ELSE 'small' END FROM far.orders "
	          "ORDER BY id");
	REQUIRE(twin.LastRead().Mentions("CASE"));
	twin.Same("SELECT id, coalesce(name, 'none') FROM far.orders ORDER BY id");
	REQUIRE(twin.LastRead().Mentions("COALESCE"));
	twin.Same("SELECT id, CAST(score AS INTEGER) FROM far.orders ORDER BY id");
	REQUIRE(twin.LastRead().Mentions("CAST"));
	twin.Same("SELECT id, amt * 2 + score FROM far.orders ORDER BY id");
	REQUIRE(twin.LastRead().Mentions("*"));
	twin.Same("SELECT id, upper(name) FROM far.orders WHERE amt > 100 AND amt < 400 ORDER BY id");
	REQUIRE(twin.LastRead().Mentions("upper"));
	twin.Same("SELECT id, unnest([id, amt]) FROM far.orders WHERE id < 3 ORDER BY 1, 2");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_UNNEST));
}

TEST_CASE("an operator the source refuses stays, with everything above it", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_operators.push_back(Op::LOGICAL_AGGREGATE_AND_GROUP_BY);
	twin.Seed();

	twin.Same("SELECT tenant, sum(amt) FROM far.orders WHERE amt > 100 GROUP BY tenant ORDER BY tenant");

	auto &call = twin.LastRead();
	REQUIRE(!call.Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(!call.Has(Op::LOGICAL_ORDER_BY));
	REQUIRE(call.Has(Op::LOGICAL_FILTER));
	REQUIRE(call.rows.size() == 4);
}

TEST_CASE("a volatile function never crosses", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT id FROM far.orders WHERE random() >= 0 ORDER BY id");

	auto &call = twin.LastRead();
	REQUIRE(!call.Has(Op::LOGICAL_FILTER));
	REQUIRE(!call.Mentions("random"));
}

TEST_CASE("a type the source refuses keeps the expression carrying it on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.store->refused_types.push_back(LogicalTypeId::UUID);
	twin.Seed();

	twin.Same("SELECT i32 FROM far.types_t WHERE u = '00000000-0000-0000-0000-000000000001'::UUID");

	auto &call = twin.LastRead();
	REQUIRE(!call.Has(Op::LOGICAL_FILTER));
}

TEST_CASE("an expression kind crossing never moves stays without asking the source", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT list_transform(l, lambda x: x + 1) FROM far.types_t");
	REQUIRE(!twin.LastRead().Mentions("list_transform"));

	twin.Same("SELECT id FROM far.orders o WHERE amt > (SELECT avg(amt) FROM far.orders) ORDER BY id");
}

TEST_CASE("every type crosses and comes back as itself", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT * FROM far.types_t");
	twin.Same("SELECT i32 FROM far.types_t WHERE b AND i8 = 1 AND i16 = 2 AND i64 = 4 AND h = 5 AND f = 1.5 AND d = "
	          "2.5 AND dec = 3.25 AND s = 'x' AND dt = DATE '2024-01-01' AND tm = TIME '12:34:56' AND ts = TIMESTAMP "
	          "'2024-01-01 12:34:56' AND iv = INTERVAL 1 DAY AND u = '00000000-0000-0000-0000-000000000001'::UUID AND "
	          "l = [1, 2]");
	REQUIRE(twin.LastRead().Has(Op::LOGICAL_FILTER));
	twin.Same("SELECT CAST(dec AS VARCHAR) FROM far.types_t");
}

TEST_CASE("set operations, distinct, subqueries and CTEs over one source cross", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT DISTINCT tenant FROM far.orders ORDER BY tenant");
	REQUIRE(twin.store->reads.size() == 1);
	twin.Same("SELECT tenant FROM far.orders UNION SELECT tenant FROM far.customers ORDER BY tenant");
	twin.Same("SELECT tenant FROM far.orders EXCEPT SELECT tenant FROM far.customers ORDER BY tenant");
	twin.Same("WITH big AS (SELECT * FROM far.orders WHERE amt > 100) SELECT count(*) FROM big");
	twin.Same("SELECT count(*) FROM far.orders o, far.customers c WHERE o.tenant = c.tenant");
	twin.Same("SELECT DISTINCT ON (tenant) tenant, id FROM far.orders ORDER BY tenant, amt DESC");
	twin.Same(
	    "SELECT o.id, c.tenant FROM far.orders o ASOF JOIN far.customers c ON o.tenant >= c.tenant ORDER BY o.id");
	twin.Same("SELECT o.id, c.cname FROM (SELECT id FROM far.orders ORDER BY id) o POSITIONAL JOIN (SELECT cname FROM "
	          "far.customers ORDER BY cname) c ORDER BY o.id");
	twin.Same("PIVOT far.sales ON quarter USING sum(amount) GROUP BY region ORDER BY region");
	twin.Same("SELECT id, sum(amt) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM far.orders "
	          "ORDER BY id");
	twin.Same("SELECT id, lag(amt, 1, 0) OVER (ORDER BY id) FROM far.orders ORDER BY id");
}

TEST_CASE("a sample that crosses returns the asked-for number of the source's rows", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	auto sampled = twin.Query("SELECT id FROM far.orders USING SAMPLE 3 ROWS");

	REQUIRE(sampled->RowCount() == 3);
	set<int32_t> ids;
	for (idx_t r = 0; r < 3; r++) {
		auto id = sampled->GetValue(0, r).GetValue<int32_t>();
		REQUIRE(id >= 1);
		REQUIRE(id <= 5);
		ids.insert(id);
	}
	REQUIRE(ids.size() == 3);
}

TEST_CASE("a column-free query still reads the source once per row", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();

	twin.Same("SELECT 1 FROM far.orders");

	REQUIRE(twin.LastRead().rows.size() == 5);
}

TEST_CASE("a join the source runs, read by an expression only the target computes, keeps both sides' columns",
          "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	auto from = GENERATE(as<string> {}, "(SELECT 2 AS id, 'z' AS n) s", "(VALUES (2, 'z')) s(id, n)");
	INFO(from);
	Twin twin(transport);
	twin.store->refused_functions.insert("upper");
	twin.Seed();

	twin.Same("SELECT upper(s.n) AS u, orders.name FROM far.orders, " + from + " WHERE orders.id = s.id");
}

TEST_CASE("a VALUES row calling a function only the target has stays on the target", "[pushdown]") {
	auto transport = EACH_TRANSPORT();
	INFO(TransportName(transport));
	Twin twin(transport);
	twin.Seed();
	// Volatile, so crossing does not turn the VALUES list into rows.
	ScalarFunction near_only(
	    "near_only", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    [](DataChunk &args, ExpressionState &, Vector &result) { result.Reference(args.data[0]); });
	near_only.SetStability(FunctionStability::VOLATILE);
	ExtensionLoader loader(*twin.near.instance, "near_only");
	loader.RegisterFunction(near_only);

	auto result = twin.Query(
	    "SELECT orders.name, s.n FROM far.orders, (VALUES (2, near_only('z'))) s(id, n) WHERE orders.id = s.id");

	REQUIRE(result->RowCount() == 1);
	REQUIRE(result->GetValue(0, 0) == Value("beta"));
	REQUIRE(result->GetValue(1, 0) == Value("z"));
}
