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

	twin.Same("SELECT list_transform(l, x -> x + 1) FROM far.types_t");
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
