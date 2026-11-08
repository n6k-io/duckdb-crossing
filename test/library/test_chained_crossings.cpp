#include "framework/twin.hpp"

using namespace duckdb;

using Op = LogicalOperatorType;

namespace {

//! db1 crosses to db2, whose `db3` is itself a crossing to db3.
struct Chain {
	shared_ptr<FarStore> db2;
	shared_ptr<FarStore> db3;
	DuckDB db1;
	Connection con;

	Chain() : db2(make_shared_ptr<FarStore>()), db3(make_shared_ptr<FarStore>()), db1(nullptr), con(db1) {
		ExtensionLoader db2_loader(*db2->db.instance, "fardb");
		FarSource::Register(db2_loader, db3);
		ExtensionLoader db1_loader(*db1.instance, "fardb");
		FarSource::Register(db1_loader, db2);

		Run(db3->con, "CREATE TABLE orders(id INTEGER PRIMARY KEY, name VARCHAR, amt INTEGER, tenant INTEGER)");
		Run(db3->con, "INSERT INTO orders VALUES (1, 'alpha', 50, 1), (2, 'beta', 150, 1), (3, 'gamma', 250, 2), "
		              "(4, NULL, 350, 2), (5, 'epsilon', 450, 3)");
		Run(db2->con, "ATTACH 'db3.example.com' AS db3 (TYPE fardb)");
		Run(con, "ATTACH 'db2.example.com' AS db2 (TYPE fardb, CATALOG 'db3')");
	}

	void Same(const string &db1_sql, const string &db3_sql) {
		INFO("db1: " << db1_sql);
		INFO("db3: " << db3_sql);
		auto expected = Collected(db3->con.Query(db3_sql));
		if (expected->HasError()) {
			FAIL(expected->GetError());
		}
		auto got = Collected(con.Query(db1_sql));
		if (got->HasError()) {
			FAIL(got->GetError());
		}
		REQUIRE(got->types == expected->types);
		REQUIRE(got->RowCount() == expected->RowCount());
		for (idx_t r = 0; r < expected->RowCount(); r++) {
			for (idx_t c = 0; c < expected->ColumnCount(); c++) {
				INFO("row " << r << " col " << c);
				REQUIRE(ValueOperations::NotDistinctFrom(got->GetValue(c, r), expected->GetValue(c, r)));
			}
		}
	}

	static void Run(Connection &on, const string &sql) {
		auto result = on.Query(sql);
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
	}
};

} // namespace

TEST_CASE("a read through two crossings runs on the last source", "[chained_crossings]") {
	Chain chain;

	chain.Same("SELECT tenant, sum(amt) FROM db2.orders WHERE amt > 100 GROUP BY tenant ORDER BY tenant",
	           "SELECT tenant, sum(amt) FROM orders WHERE amt > 100 GROUP BY tenant ORDER BY tenant");

	REQUIRE(chain.db2->reads.size() == 1);
	auto &hop1 = chain.db2->LastRead();
	REQUIRE(hop1.Has(Op::LOGICAL_FILTER));
	REQUIRE(hop1.Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(hop1.Has(Op::LOGICAL_ORDER_BY));

	REQUIRE(chain.db3->reads.size() == 1);
	auto &hop2 = chain.db3->LastRead();
	REQUIRE(hop2.Has(Op::LOGICAL_FILTER));
	REQUIRE(hop2.Has(Op::LOGICAL_AGGREGATE_AND_GROUP_BY));
	REQUIRE(hop2.Has(Op::LOGICAL_ORDER_BY));
	REQUIRE(hop2.rows.size() == 3);
}

TEST_CASE("a plain scan through two crossings reads the last source once", "[chained_crossings]") {
	Chain chain;

	chain.Same("SELECT * FROM db2.orders ORDER BY id", "SELECT * FROM orders ORDER BY id");

	REQUIRE(chain.db2->reads.size() == 1);
	REQUIRE(chain.db3->reads.size() == 1);
	REQUIRE(chain.db3->LastRead().rows.size() == 5);
}
