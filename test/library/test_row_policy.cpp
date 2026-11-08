#include "framework/twin.hpp"

#include <map>

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

using namespace duckdb;

namespace {

//! Rows 1 and 2 have tenant 1 and are visible; row 3 has tenant 2, is hidden, and its v fails a cast to INTEGER.
struct Policed {
	Policed() {
		twin.store->returning = true;
		twin.store->declare_key_constraints = true;
		twin.Far("CREATE TABLE p(id INTEGER PRIMARY KEY, x INTEGER, v VARCHAR, tenant INTEGER)");
		twin.Far("INSERT INTO p VALUES (1, 1, '1', 1), (2, 2, '2', 1), (3, 3, 'oops', 2)");
		twin.Far("CREATE TABLE q(id INTEGER)");
		twin.Far("INSERT INTO q VALUES (1), (2), (3)");
		twin.Far("CREATE TABLE allowed(tenant INTEGER)");
		twin.Far("INSERT INTO allowed VALUES (1), (3)");
		auto &far_context = *twin.store->con.context;
		twin.store->floors = [this, &far_context](const CrossingFloor &floor) -> unique_ptr<TableRef> {
			if (floor.table != "p") {
				return nullptr;
			}
			asked.push_back(floor);
			if (!polices(floor)) {
				return nullptr;
			}
			Parser parser(far_context);
			if (!body.empty()) {
				parser.ParseQuery(body);
			} else {
				auto entry = verb_policies.find(floor.verb);
				parser.ParseQuery("SELECT *, rowid FROM memory.main.p WHERE " +
				                  (entry == verb_policies.end() ? policy : entry->second));
			}
			auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
			return make_uniq<SubqueryRef>(std::move(select), Identifier(floor.table));
		};
	}

	void Attach() {
		twin.Attach();
	}

	Value Far(const string &sql) {
		auto result = twin.store->con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		return result->Fetch()->GetValue(0, 0);
	}

	bool HiddenUntouched() {
		return Far("SELECT count(*) FROM p WHERE id = 3 AND x = 3 AND v = 'oops' AND tenant = 2") == Value::BIGINT(1) &&
		       Far("SELECT count(*) FROM p WHERE id = 3") == Value::BIGINT(1);
	}

	//! Every row on far as id:x:v:tenant, in id order.
	string Rows() {
		return Far("SELECT string_agg(id || ':' || x || ':' || v || ':' || tenant, ' ' ORDER BY id) FROM p").ToString();
	}

	void Runs(const string &sql) {
		auto result = twin.con.Query(sql);
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
	}

	bool Asked(CrossingVerb verb, bool reads_rows) {
		for (auto &floor : asked) {
			if (floor.verb == verb && floor.reads_rows == reads_rows) {
				return true;
			}
		}
		return false;
	}

	Twin twin;
	vector<CrossingFloor> asked;
	//! What the policy keeps, over memory.main.p.
	string policy = "tenant = 1";
	//! Replaces `policy` for a floor of this verb.
	std::map<CrossingVerb, string> verb_policies;
	//! Replaces the whole policy query when set.
	string body;
	std::function<bool(const CrossingFloor &)> polices = [](const CrossingFloor &) {
		return true;
	};
};

} // namespace

TEST_CASE("a row policy hides rows from a read, and its WHERE never runs on them", "[policy]") {
	Policed p;
	p.Attach();

	REQUIRE(p.twin.Query("SELECT count(*) FROM far.p")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.twin.Query("SELECT count(*) FROM far.p WHERE v::INTEGER > 0")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.Asked(CrossingVerb::SELECT, true));
}

TEST_CASE("an update's WHERE never runs on a row its policy hides", "[policy]") {
	Policed p;
	p.Attach();

	REQUIRE(p.twin.Query("UPDATE far.p SET x = x + 100 WHERE v::INTEGER > 0")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.Asked(CrossingVerb::UPDATE, true));
	REQUIRE(p.HiddenUntouched());
}

TEST_CASE("a delete's WHERE never runs on a row its policy hides", "[policy]") {
	Policed p;
	p.Attach();

	REQUIRE(p.twin.Query("DELETE FROM far.p WHERE v::INTEGER > 0")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.Asked(CrossingVerb::DELETE_, true));
	REQUIRE(p.HiddenUntouched());
}

TEST_CASE("an update or delete changes and returns only rows its policy allows", "[policy]") {
	Policed p;
	p.Attach();

	auto updated = p.twin.Query("UPDATE far.p SET x = x + 100 RETURNING id");
	REQUIRE(updated->RowCount() == 2);
	REQUIRE(p.Asked(CrossingVerb::UPDATE, true));
	REQUIRE(p.HiddenUntouched());

	auto deleted = p.twin.Query("DELETE FROM far.p WHERE id > 0 RETURNING id");
	REQUIRE(deleted->RowCount() == 2);
	REQUIRE(p.Asked(CrossingVerb::DELETE_, true));
	REQUIRE(p.HiddenUntouched());
}

TEST_CASE("a write reading no rows needs no SELECT and is held only by its write policy", "[policy]") {
	Policed p;
	p.twin.store->verbs["p"] = {CrossingVerb::UPDATE, CrossingVerb::DELETE_};
	p.polices = [](const CrossingFloor &floor) {
		return floor.verb != CrossingVerb::SELECT;
	};
	p.Attach();

	REQUIRE(p.twin.Query("UPDATE far.p SET x = 0")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.Asked(CrossingVerb::UPDATE, false));
	REQUIRE_FALSE(p.Asked(CrossingVerb::UPDATE, true));
	REQUIRE(p.HiddenUntouched());

	REQUIRE(p.twin.Query("DELETE FROM far.p")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.Asked(CrossingVerb::DELETE_, false));
	REQUIRE(p.HiddenUntouched());
}

TEST_CASE("a write reading its target's rows needs SELECT", "[policy]") {
	Policed p;
	p.twin.store->verbs["p"] = {CrossingVerb::UPDATE, CrossingVerb::DELETE_};
	p.Attach();

	p.twin.Error("UPDATE far.p SET x = x + 1");
	p.twin.Error("UPDATE far.p SET x = 0 WHERE id = 1");
	p.twin.Error("UPDATE far.p SET x = 0 RETURNING id");
	p.twin.Error("DELETE FROM far.p WHERE id = 1");
	REQUIRE(p.HiddenUntouched());
	REQUIRE(p.Far("SELECT sum(x) FROM p") == Value::HUGEINT(6));
}

TEST_CASE("a write with a subquery reaches only rows its policy allows", "[policy]") {
	Policed p;
	p.Attach();

	REQUIRE(p.twin.Query("DELETE FROM far.p WHERE id IN (SELECT id FROM far.q)")->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.HiddenUntouched());
}

TEST_CASE("a merge matching a hidden row neither changes it nor inserts it again", "[policy]") {
	Policed p;
	SECTION("under a read and write policy") {
	}
	SECTION("under a write policy alone") {
		p.polices = [](const CrossingFloor &floor) {
			return floor.verb != CrossingVerb::SELECT;
		};
	}
	p.Attach();

	p.twin.con.Query("MERGE INTO far.p t USING (SELECT 3 AS id) s ON t.id = s.id "
	                 "WHEN MATCHED THEN UPDATE SET x = 99 "
	                 "WHEN NOT MATCHED THEN INSERT VALUES (s.id, 0, '0', 1)");
	REQUIRE(p.Rows() == "1:1:1:1 2:2:2:1 3:3:oops:2");
}

TEST_CASE("a merge's not-matched-by-source actions change only rows its policy allows", "[policy]") {
	Policed p;
	SECTION("under a read and write policy") {
	}
	SECTION("under a write policy alone") {
		p.polices = [](const CrossingFloor &floor) {
			return floor.verb != CrossingVerb::SELECT;
		};
	}
	p.Attach();

	p.Runs(
	    "MERGE INTO far.p t USING (SELECT 1 AS id) s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN UPDATE SET x = 99");
	REQUIRE(p.Rows() == "1:1:1:1 2:99:2:1 3:3:oops:2");
	p.Runs("MERGE INTO far.p t USING (SELECT 1 AS id) s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE");
	REQUIRE(p.Rows() == "1:1:1:1 3:3:oops:2");
}

TEST_CASE("a merge or ON CONFLICT into a policed table is refused and changes nothing", "[policy]") {
	Policed p;
	SECTION("under a read and write policy") {
	}
	SECTION("under a write policy alone") {
		p.polices = [](const CrossingFloor &floor) {
			return floor.verb != CrossingVerb::SELECT;
		};
	}
	SECTION("under an update policy alone") {
		p.polices = [](const CrossingFloor &floor) {
			return floor.verb == CrossingVerb::UPDATE;
		};
	}
	SECTION("under a delete policy alone") {
		p.polices = [](const CrossingFloor &floor) {
			return floor.verb == CrossingVerb::DELETE_;
		};
	}
	p.Attach();

	auto before = p.Rows();
	for (auto &sql : {"MERGE INTO far.p t USING far.q s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 99",
	                  "MERGE INTO far.p t USING far.q s ON t.id = s.id WHEN MATCHED THEN DELETE",
	                  "MERGE INTO far.p t USING far.q s ON t.id = s.id WHEN MATCHED AND t.v::INTEGER > 0 THEN UPDATE "
	                  "SET x = 99",
	                  "INSERT INTO far.p VALUES (1, 0, '0', 1) ON CONFLICT (id) DO UPDATE SET x = 99",
	                  "INSERT INTO far.p VALUES (3, 0, '0', 1), (4, 4, '4', 1) ON CONFLICT DO NOTHING"}) {
		INFO(sql);
		REQUIRE_THAT(p.twin.Error(sql), Catch::Contains("cannot apply its row policy"));
		REQUIRE(p.Rows() == before);
	}
}

TEST_CASE("a merge or ON CONFLICT runs when no verb on its table has a policy", "[policy]") {
	Policed p;
	p.polices = [](const CrossingFloor &) {
		return false;
	};
	p.Attach();

	p.Runs("MERGE INTO far.p t USING far.q s ON t.id = s.id WHEN MATCHED AND t.id < 3 THEN UPDATE SET x = 99");
	REQUIRE(p.Rows() == "1:99:1:1 2:99:2:1 3:3:oops:2");
	p.Runs("INSERT INTO far.p VALUES (4, 4, '4', 1) ON CONFLICT (id) DO UPDATE SET x = 0");
	REQUIRE(p.Rows() == "1:99:1:1 2:99:2:1 3:3:oops:2 4:4:4:1");
}

namespace {

//! Each read touches v, which fails a cast on the hidden row, so any error means the user's expression saw it.
void ReadsOnlyVisibleRows(Policed &p) {
	struct Read {
		string sql;
		string expected;
	};
	vector<Read> reads = {
	    {"SELECT count(*) FROM far.p WHERE v::INTEGER > 0", "2"},
	    {"SELECT count(*) FROM far.p WHERE id > 0 AND v::INTEGER > 0", "2"},
	    {"SELECT count(*) FROM far.p WHERE v::INTEGER > 0 AND id > 0", "2"},
	    {"SELECT count(*) FROM far.p WHERE v::INTEGER > 0 OR id < 0", "2"},
	    {"SELECT count(*) FROM far.p WHERE tenant = 2 OR v::INTEGER > 0", "2"},
	    {"SELECT count(*) FROM far.p WHERE id = 3 AND v::INTEGER > 0", "0"},
	    {"SELECT count(*) FROM far.p WHERE v::INTEGER IN (1, 2)", "2"},
	    {"SELECT count(*) FROM far.p WHERE CASE WHEN id > 0 THEN v::INTEGER > 0 END", "2"},
	    {"SELECT sum(v::INTEGER) FROM far.p", "3"},
	    {"SELECT string_agg(v::INTEGER::VARCHAR, ',' ORDER BY id) FROM far.p", "1,2"},
	    {"SELECT count(*) FROM (SELECT v::INTEGER AS n FROM far.p) WHERE n > 0", "2"},
	    {"SELECT count(*) FROM (SELECT v::INTEGER AS n FROM far.p GROUP BY n)", "2"},
	    {"SELECT count(*) FROM far.p GROUP BY tenant HAVING min(v::INTEGER) > 0", "2"},
	    {"SELECT max(n) FROM (SELECT v::INTEGER AS n FROM far.p ORDER BY n LIMIT 5)", "2"},
	    {"SELECT count(*) FROM far.p JOIN far.q ON q.id = p.v::INTEGER", "2"},
	    {"SELECT count(*) FROM far.q WHERE q.id IN (SELECT v::INTEGER FROM far.p)", "2"},
	    {"SELECT count(*) FROM far.q WHERE EXISTS (SELECT 1 FROM far.p WHERE p.v::INTEGER = q.id)", "2"},
	    {"SELECT count(*) FROM far.p WHERE id IN (SELECT id FROM far.q) AND v::INTEGER > 0", "2"},
	    {"SELECT count(*) FROM far.p a JOIN far.p b ON a.id = b.id WHERE b.v::INTEGER > 0", "2"},
	    {"SELECT count(*) FROM (SELECT * FROM far.p UNION ALL SELECT * FROM far.p) WHERE v::INTEGER > 0", "4"},
	};
	for (auto &read : reads) {
		auto result = p.twin.con.Query(read.sql);
		INFO("policy: " << p.policy);
		INFO(read.sql);
		if (result->HasError()) {
			CHECK(result->GetError() == "");
			continue;
		}
		CHECK(result->Fetch()->GetValue(0, 0).ToString() == read.expected);
	}
}

} // namespace

TEST_CASE("a read's expressions never run on a row its policy hides", "[policy]") {
	Policed p;
	SECTION("a comparison on a column") {
		p.policy = "tenant = 1";
	}
	SECTION("a comparison on another type") {
		p.policy = "tenant::VARCHAR = '1'";
	}
	SECTION("an expensive function") {
		p.policy = "regexp_matches(tenant::VARCHAR, '^1$')";
	}
	SECTION("an OR") {
		p.policy = "tenant = 1 OR tenant = 0";
	}
	SECTION("a membership table") {
		p.policy = "tenant IN (SELECT tenant FROM memory.main.allowed)";
	}
	SECTION("a correlated EXISTS") {
		p.policy = "EXISTS (SELECT 1 FROM memory.main.allowed a WHERE a.tenant = p.tenant)";
	}
	SECTION("an inner join") {
		p.body = "SELECT p.id, p.x, p.v, p.tenant, p.rowid FROM memory.main.p JOIN memory.main.allowed a "
		         "ON a.tenant = p.tenant";
	}
	SECTION("far's optimizer is off") {
		p.twin.Far("SET GLOBAL enable_optimizer = false");
	}
	SECTION("far's pushdown and join order are off") {
		p.twin.Far("SET GLOBAL disabled_optimizers = 'filter_pushdown,join_order'");
	}
	p.Attach();
	ReadsOnlyVisibleRows(p);
}

TEST_CASE("a write's WHERE never runs on a hidden row, whichever conjunct comes first", "[policy]") {
	Policed p;
	SECTION("a cast") {
	}
	SECTION("a membership table") {
		p.policy = "tenant IN (SELECT tenant FROM memory.main.allowed)";
	}
	SECTION("far's optimizer is off") {
		p.twin.Far("SET GLOBAL enable_optimizer = false");
	}
	p.Attach();

	for (auto &where : {"id = 3 AND v::INTEGER > 0", "v::INTEGER > 0 AND id = 3",
	                    "id = 3 AND error('saw ' || v) IS NULL", "id = 3 AND v::INTEGER > 0 RETURNING id"}) {
		INFO(where);
		CHECK(p.twin.Query(string("UPDATE far.p SET x = 0 WHERE ") + where)->RowCount() <= 1);
		CHECK(p.twin.Query(string("DELETE FROM far.p WHERE ") + where)->RowCount() <= 1);
		REQUIRE(p.HiddenUntouched());
	}
}

TEST_CASE("a read's error() never runs on a row its policy hides", "[policy]") {
	Policed p;
	p.Attach();

	REQUIRE(p.twin.Query("SELECT count(*) FROM far.p WHERE id = 3 AND error('saw ' || v) IS NULL")->GetValue(0, 0) ==
	        Value::BIGINT(0));
	REQUIRE(
	    p.twin.Query("SELECT count(*) FROM far.p WHERE tenant = 2 AND error('saw ' || v) IS NULL")->GetValue(0, 0) ==
	    Value::BIGINT(0));
}

TEST_CASE("a CTE reading a policed table twice sees only allowed rows", "[policy]") {
	Policed p;
	p.Attach();

	REQUIRE(p.twin
	            .Query("WITH c AS (SELECT * FROM far.p) SELECT (SELECT count(*) FROM c WHERE v::INTEGER > 0) + "
	                   "(SELECT count(*) FROM c WHERE id = 3 AND v::INTEGER > 0)")
	            ->GetValue(0, 0) == Value::BIGINT(2));
	REQUIRE(p.twin
	            .Query("WITH c AS MATERIALIZED (SELECT * FROM far.p) SELECT count(*) FROM c a JOIN c b ON a.id = b.id "
	                   "WHERE b.v::INTEGER > 0")
	            ->GetValue(0, 0) == Value::BIGINT(2));
}

TEST_CASE("a write's target takes its write policy, and the rows it reads elsewhere take SELECT", "[policy]") {
	Policed p;
	p.verb_policies[CrossingVerb::UPDATE] = "id <> 2";
	p.Attach();

	REQUIRE(p.twin.Query("UPDATE far.p SET x = 0 WHERE id IN (SELECT id FROM far.p)")->GetValue(0, 0) ==
	        Value::BIGINT(1));
	REQUIRE(p.Rows() == "1:0:1:1 2:2:2:1 3:3:oops:2");
}

TEST_CASE("a row policy holds over many tenants and rows", "[policy]") {
	Policed p;
	p.Far("INSERT INTO p SELECT i, i, CASE WHEN i % 7 = 1 THEN i::VARCHAR ELSE 'x' || i END, i % 7 FROM range(10, "
	      "20000) t(i)");
	p.Attach();

	REQUIRE(p.twin.Query("SELECT count(*) FROM far.p WHERE v::INTEGER > 0")->GetValue(0, 0) ==
	        p.Far("SELECT count(*) FROM p WHERE tenant = 1"));
	REQUIRE(p.twin.Query("SELECT count(*) FROM far.p WHERE id = 12 AND v::INTEGER > 0")->GetValue(0, 0) ==
	        Value::BIGINT(0));
	REQUIRE(p.twin.Query("SELECT count(*) FROM far.p WHERE id = 15 AND v::INTEGER > 0")->GetValue(0, 0) ==
	        Value::BIGINT(1));
	REQUIRE(p.twin.Query("UPDATE far.p SET x = -1 WHERE id BETWEEN 10 AND 100 AND v::INTEGER > 0")->GetValue(0, 0) ==
	        p.Far("SELECT count(*) FROM p WHERE id BETWEEN 10 AND 100 AND tenant = 1"));
	REQUIRE(p.Far("SELECT count(*) FROM p WHERE x = -1 AND tenant <> 1") == Value::BIGINT(0));
}
