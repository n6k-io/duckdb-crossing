#include "framework/twin.hpp"

#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;

namespace {

void AttachPair(Twin &twin, const string &a_group, const string &b_group) {
	for (auto &attach : {std::make_pair("a", a_group), std::make_pair("b", b_group)}) {
		auto group = attach.second.empty() ? "" : ", GROUP '" + attach.second + "'";
		twin.Query("ATTACH 'far.example.com' AS " + string(attach.first) + " (TYPE fardb" + group + ")");
	}
}

constexpr const char *JOIN_SQL = "SELECT o.id, c.cname FROM a.orders o JOIN b.customers c ON o.tenant = c.tenant";
constexpr const char *FAR_JOIN_SQL = "SELECT o.id, c.cname FROM orders o JOIN customers c ON o.tenant = c.tenant";

} // namespace

TEST_CASE("a join of two attaches in one group is one read", "[groups]") {
	auto transport = EACH_TRANSPORT();
	Twin twin(transport);
	twin.Seed();
	AttachPair(twin, "g", "g");
	twin.store->reads.clear();

	twin.Same(string(JOIN_SQL) + " ORDER BY o.id", string(FAR_JOIN_SQL) + " ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 1);
	REQUIRE(twin.LastRead().Has(LogicalOperatorType::LOGICAL_COMPARISON_JOIN));
}

TEST_CASE("a join of attaches outside one group reads each apart", "[groups]") {
	auto transport = EACH_TRANSPORT();
	auto groups = GENERATE(std::make_pair("", ""), std::make_pair("g", ""), std::make_pair("g", "h"));
	INFO(TransportName(transport) << " groups '" << groups.first << "' '" << groups.second << "'");
	Twin twin(transport);
	twin.Seed();
	AttachPair(twin, groups.first, groups.second);
	twin.store->reads.clear();

	twin.Same(string(JOIN_SQL) + " ORDER BY o.id", string(FAR_JOIN_SQL) + " ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 2);
	for (auto &read : twin.store->reads) {
		REQUIRE(!read.Has(LogicalOperatorType::LOGICAL_COMPARISON_JOIN));
	}
}

TEST_CASE("a view over attaches in one group is accepted", "[groups][view]") {
	auto transport = EACH_TRANSPORT();
	Twin twin(transport);
	twin.Seed();
	AttachPair(twin, "g", "g");

	SECTION("join of both") {
		twin.Query("CREATE VIEW a.main.v AS " + string(JOIN_SQL));
		twin.Same("SELECT id, cname FROM a.v ORDER BY id", string(FAR_JOIN_SQL) + " ORDER BY o.id");
	}
	SECTION("body only in the other attach") {
		twin.Query("CREATE VIEW a.main.v AS SELECT tenant, cname FROM b.customers");
		twin.Same("SELECT tenant, cname FROM a.v ORDER BY tenant", "SELECT tenant, cname FROM customers ORDER BY 1");
	}
	REQUIRE(twin.store->ddls.size() == 1);
}

TEST_CASE("a view whose body is outside the owner's group is refused", "[groups][view]") {
	auto transport = EACH_TRANSPORT();
	Twin twin(transport);
	twin.Seed();
	AttachPair(twin, "g", "h");

	for (auto sql : {"CREATE VIEW a.main.v AS SELECT tenant, cname FROM b.customers",
	                 "CREATE VIEW a.main.v AS SELECT o.id, c.cname FROM a.orders o JOIN b.customers c ON o.tenant = "
	                 "c.tenant"}) {
		INFO(sql);
		REQUIRE_THAT(twin.Error(sql), Catch::Contains("does not run wholly on the source"));
	}
	REQUIRE(twin.store->ddls.empty());
}

namespace {

void SeedSecondCatalog(Twin &twin) {
	twin.Far("ATTACH ':memory:' AS far2");
	twin.Far("CREATE TABLE far2.main.customers(tenant INTEGER PRIMARY KEY, cname VARCHAR)");
	twin.Far("INSERT INTO far2.main.customers VALUES (1, 'umbrella'), (3, 'hooli')");
	twin.Query("ATTACH 'far.example.com' AS a (TYPE fardb, GROUP 'g', CATALOG 'memory')");
	twin.Query("ATTACH 'far.example.com' AS b (TYPE fardb, GROUP 'g', CATALOG 'far2')");
}

constexpr const char *FAR_CATALOGS_JOIN_SQL =
    "SELECT o.id, c.cname FROM memory.main.orders o JOIN far2.main.customers c ON o.tenant = c.tenant";

} // namespace

TEST_CASE("a join across catalogs of one group reads each table from its own catalog", "[groups]") {
	auto transport = EACH_TRANSPORT();
	Twin twin(transport);
	twin.Seed();
	SeedSecondCatalog(twin);
	twin.store->reads.clear();

	twin.Same(string(JOIN_SQL) + " ORDER BY o.id", string(FAR_CATALOGS_JOIN_SQL) + " ORDER BY o.id");

	REQUIRE(twin.store->reads.size() == 1);
	auto &read = twin.LastRead();
	REQUIRE(read.Has(LogicalOperatorType::LOGICAL_COMPARISON_JOIN));
	vector<string> catalogs;
	for (auto &use : read.tables) {
		catalogs.push_back(use.catalog);
	}
	std::sort(catalogs.begin(), catalogs.end());
	REQUIRE(catalogs == vector<string> {"far2", "memory"});
	if (transport == Transport::SUBSTRAIT) {
		REQUIRE(read.wire.find("[\"far2\",\"main\",\"customers\"]") != string::npos);
	}
}

TEST_CASE("a view across catalogs of one group reads each table from its own catalog", "[groups][view]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();
	SeedSecondCatalog(twin);

	twin.Query("CREATE VIEW a.main.v AS " + string(JOIN_SQL));

	twin.Same("SELECT id, cname FROM a.v ORDER BY id", string(FAR_CATALOGS_JOIN_SQL) + " ORDER BY o.id");
}

TEST_CASE("the receiver's catalog check refuses a plan naming a catalog", "[groups]") {
	Twin twin(Transport::SUBSTRAIT);
	twin.Seed();
	SeedSecondCatalog(twin);
	twin.store->catalog_check = [](const string &catalog) {
		if (catalog == "far2") {
			throw CatalogException("far: '%s' is not served here", catalog);
		}
	};

	REQUIRE_THAT(twin.Error(JOIN_SQL), Catch::Contains("'far2' is not served here"));
	twin.Same("SELECT id FROM a.orders ORDER BY id", "SELECT id FROM orders ORDER BY id");
}

TEST_CASE("floors in different catalogs are different floors", "[groups]") {
	auto here = MakeFloorNode(0, "main", "customers", {"tenant"}, {LogicalType::INTEGER}, "memory");
	auto there = MakeFloorNode(0, "main", "customers", {"tenant"}, {LogicalType::INTEGER}, "far2");
	auto &here_data = *here->Cast<LogicalGet>().bind_data;
	REQUIRE(here_data.Equals(*here_data.Copy()));
	REQUIRE(!here_data.Equals(*there->Cast<LogicalGet>().bind_data));
}

TEST_CASE("a join after a write in a transaction reads through the written attach", "[groups][transaction]") {
	auto transport = EACH_TRANSPORT();
	auto written_first = GENERATE(false, true);
	INFO(TransportName(transport) << (written_first ? " written attach first" : " written attach second"));
	Twin twin(transport);
	twin.Seed();
	SeedSecondCatalog(twin);
	auto sql = written_first
	               ? "SELECT o.id, c.cname FROM b.customers c JOIN a.orders o ON o.tenant = c.tenant ORDER BY "
	                 "o.id"
	               : string(JOIN_SQL) + " ORDER BY o.id";

	twin.Query("BEGIN");
	twin.Query("INSERT INTO b.customers VALUES (2, 'initrode')");
	twin.store->reads.clear();
	auto result = twin.Query(sql);
	twin.Query("COMMIT");

	REQUIRE(twin.store->reads.size() == 1);
	vector<string> names;
	for (idx_t r = 0; r < result->RowCount(); r++) {
		names.push_back(result->GetValue(1, r).ToString());
	}
	REQUIRE(names == vector<string> {"umbrella", "umbrella", "initrode", "initrode", "hooli"});
}
