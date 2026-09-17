#include "catch.hpp"
#include "crossing_substrait.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

namespace {

struct Fixture {
	DuckDB db;
	Connection con;

	Fixture() : db(nullptr), con(db) {
		Run("CREATE TABLE orders(id INTEGER, name VARCHAR, amt INTEGER, tenant INTEGER, score DOUBLE, ts TIMESTAMP)");
		Run("INSERT INTO orders VALUES (1, 'alpha', 50, 1, 1.5, '2024-01-01 00:00:00'), (2, 'beta', 150, 1, 2.5, "
		    "'2024-01-02 00:00:00'), (3, 'gamma', 250, 2, 3.5, '2024-01-03 00:00:00'), (4, NULL, 350, 2, NULL, NULL), "
		    "(5, 'epsilon', 450, 3, 5.5, '2024-01-05 00:00:00')");
		Run("CREATE TABLE customers(tenant INTEGER, cname VARCHAR)");
		Run("INSERT INTO customers VALUES (1, 'acme'), (2, 'globex'), (4, 'initech')");
		Run("CREATE TABLE sales(region VARCHAR, quarter VARCHAR, amount INTEGER)");
		Run("INSERT INTO sales VALUES ('east', 'q1', 10), ('east', 'q2', 20), ('west', 'q1', 5), ('west', 'q3', 7)");
		Run("CREATE TABLE types_t(b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, h HUGEINT, f FLOAT, d "
		    "DOUBLE, dec DECIMAL(10,2), s VARCHAR, bl BLOB, dt DATE, tm TIME, ts TIMESTAMP, tstz TIMESTAMPTZ, iv "
		    "INTERVAL, l INTEGER[], u UUID)");
		Run("INSERT INTO types_t VALUES (true, 1, 2, 3, 4, 5, 1.5, 2.5, 3.25, 'x', '\\x00\\x01'::BLOB, '2024-01-01', "
		    "'12:34:56', '2024-01-01 12:34:56', '2024-01-01 12:34:56+00', INTERVAL 1 DAY, [1, 2], "
		    "'00000000-0000-0000-0000-000000000001')");
	}

	void Run(const string &sql) {
		auto result = con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
	}

	string Render(const string &sql) {
		Connection planning(db);
		planning.BeginTransaction();
		string json;
		try {
			auto statements = planning.ExtractStatements(sql);
			REQUIRE(!statements.empty());
			for (idx_t i = 0; i + 1 < statements.size(); i++) {
				auto result = planning.Query(std::move(statements[i]));
				if (result->HasError()) {
					FAIL(result->GetError());
				}
			}
			Planner planner(*planning.context);
			planner.CreatePlan(std::move(statements.back()));
			auto plan = std::move(planner.plan);
			plan->ResolveOperatorTypes();
			json = RenderSubstraitJson(*plan);
		} catch (...) {
			planning.Rollback();
			throw;
		}
		planning.Rollback();
		return json;
	}

	unique_ptr<MaterializedQueryResult> Decoded(const string &sql) {
		auto json = Render(sql);
		Connection decoding(db);
		auto catalog = DatabaseManager::GetDefaultDatabase(*decoding.context);
		auto rel = DecodeSubstraitJson(decoding, catalog, json, "");
		auto result = rel->Execute();
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
	}

	void RoundTrip(const string &sql) {
		auto expected = con.Query(sql);
		if (expected->HasError()) {
			FAIL(expected->GetError());
		}
		auto got = Decoded(sql);
		REQUIRE(got->RowCount() == expected->RowCount());
		REQUIRE(got->ColumnCount() == expected->ColumnCount());
		for (idx_t r = 0; r < expected->RowCount(); r++) {
			for (idx_t c = 0; c < expected->ColumnCount(); c++) {
				auto want = expected->GetValue(c, r);
				auto have = got->GetValue(c, r).DefaultCastAs(want.type());
				INFO(sql << " row " << r << " col " << c);
				REQUIRE(have.ToString() == want.ToString());
			}
		}
	}
};

} // namespace

TEST_CASE("substrait: shape", "[substrait]") {
	Fixture f;
	auto json = f.Render("SELECT id FROM orders WHERE amt > 100");
	REQUIRE(json.find("\"namedTable\":{\"names\":[\"main\",\"orders\"]}") != string::npos);
	REQUIRE(json.find("\"filter\":") != string::npos);
	REQUIRE(json.find("\"name\":\"gt\"") != string::npos);
}

TEST_CASE("substrait: reads round-trip", "[substrait]") {
	Fixture f;
	f.RoundTrip("SELECT id FROM orders WHERE amt > 100 ORDER BY id");
	f.RoundTrip("SELECT id, upper(name) FROM orders WHERE amt > 100 AND amt < 400 ORDER BY id");
	f.RoundTrip("SELECT id FROM orders ORDER BY amt DESC LIMIT 2");
	f.RoundTrip("SELECT id FROM orders ORDER BY amt DESC LIMIT 2 OFFSET 1");
	f.RoundTrip("SELECT tenant, sum(amt) FROM orders GROUP BY tenant ORDER BY tenant");
	f.RoundTrip("SELECT count(*) FROM orders");
	f.RoundTrip("SELECT tenant, count(DISTINCT name) FROM orders GROUP BY tenant ORDER BY tenant");
	f.RoundTrip("SELECT o.id, c.cname FROM orders o JOIN customers c ON o.tenant = c.tenant ORDER BY o.id");
	f.RoundTrip("SELECT o.id, c.cname FROM orders o LEFT JOIN customers c ON o.tenant = c.tenant ORDER BY o.id");
	f.RoundTrip("SELECT DISTINCT tenant FROM orders ORDER BY tenant");
	f.RoundTrip("SELECT tenant FROM orders UNION SELECT tenant FROM customers ORDER BY tenant");
	f.RoundTrip("SELECT tenant FROM orders EXCEPT SELECT tenant FROM customers ORDER BY tenant");
	f.RoundTrip("SELECT id FROM orders WHERE name IS NULL OR score IS NULL ORDER BY id");
	f.RoundTrip("SELECT id FROM orders WHERE amt BETWEEN 100 AND 300 ORDER BY id");
	f.RoundTrip("SELECT id FROM orders WHERE id IN (1, 3, 5) ORDER BY id");
	f.RoundTrip("SELECT id FROM orders WHERE NOT (id IN (1, 3, 5)) ORDER BY id");
	f.RoundTrip("SELECT id, CASE WHEN amt > 300 THEN 'big' WHEN amt > 100 THEN 'mid' ELSE 'small' END FROM orders "
	            "ORDER BY id");
	f.RoundTrip("SELECT id, coalesce(name, 'none') FROM orders ORDER BY id");
	f.RoundTrip("SELECT id, CAST(score AS INTEGER) FROM orders ORDER BY id");
	f.RoundTrip("SELECT id FROM orders WHERE ts > TIMESTAMP '2024-01-02 12:00:00' ORDER BY id");
	f.RoundTrip("SELECT id FROM orders WHERE name LIKE '%a' ORDER BY id");
	f.RoundTrip("SELECT id, amt * 2 + score FROM orders ORDER BY id");
	f.RoundTrip("SELECT id FROM orders WHERE amt > 100 AND name IS NOT NULL AND (tenant = 1 OR tenant = 3) ORDER BY id");
	f.RoundTrip("SELECT count(*) FROM orders o, customers c WHERE o.tenant = c.tenant");
}

TEST_CASE("substrait: type matrix", "[substrait]") {
	Fixture f;
	f.RoundTrip("SELECT * FROM types_t");
	f.RoundTrip("SELECT i32 FROM types_t WHERE b AND i8 = 1 AND i16 = 2 AND i64 = 4 AND h = 5 AND f = 1.5 AND d = 2.5 "
	            "AND dec = 3.25 AND s = 'x' AND dt = DATE '2024-01-01' AND tm = TIME '12:34:56' AND ts = TIMESTAMP "
	            "'2024-01-01 12:34:56' AND iv = INTERVAL 1 DAY AND u = "
	            "'00000000-0000-0000-0000-000000000001'::UUID AND l = [1, 2]");
	f.RoundTrip("SELECT CAST(dec AS VARCHAR) FROM types_t");
}

TEST_CASE("substrait: window, unnest, sample, distinct on, asof, positional, pivot", "[substrait]") {
	Fixture f;
	f.RoundTrip("SELECT id, row_number() OVER (PARTITION BY tenant ORDER BY amt DESC) FROM orders ORDER BY id");
	f.RoundTrip("SELECT id, sum(amt) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM orders ORDER "
	            "BY id");
	f.RoundTrip("SELECT id, lag(amt, 1, 0) OVER (ORDER BY id) FROM orders ORDER BY id");
	f.RoundTrip("SELECT id, unnest([id, amt]) FROM orders WHERE id < 3 ORDER BY 1, 2");
	REQUIRE(f.Decoded("SELECT id FROM orders USING SAMPLE 3 ROWS")->RowCount() == 3);
	REQUIRE(f.Decoded("SELECT id FROM orders USING SAMPLE 100 PERCENT (bernoulli)")->RowCount() == 5);
	f.RoundTrip("SELECT DISTINCT ON (tenant) tenant, id FROM orders ORDER BY tenant, amt DESC");
	f.RoundTrip("SELECT o.id, c.tenant FROM orders o ASOF JOIN customers c ON o.tenant >= c.tenant ORDER BY o.id");
	f.RoundTrip("SELECT o.id, c.cname FROM (SELECT id FROM orders ORDER BY id) o POSITIONAL JOIN (SELECT cname FROM "
	            "customers ORDER BY cname) c ORDER BY o.id");
	f.RoundTrip("PIVOT sales ON quarter USING sum(amount) GROUP BY region ORDER BY region");
}
