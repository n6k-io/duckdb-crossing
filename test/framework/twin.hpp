#pragma once

#include "catch.hpp"
#include "far_source/far_source.hpp"

#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>

namespace duckdb {

#define EACH_TRANSPORT() GENERATE(Transport::NATIVE, Transport::SUBSTRAIT)

struct Twin {
	shared_ptr<FarStore> store;
	DuckDB near;
	Connection con;

	explicit Twin(Transport transport) : store(make_shared_ptr<FarStore>(transport)), near(nullptr), con(near) {
		ExtensionLoader loader(*near.instance, "fardb");
		FarSource::Register(loader, store);
	}

	~Twin() {
		store->JoinArrivals();
	}

	void Far(const string &sql) {
		auto result = store->con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
	}

	void Both(const string &sql) {
		Far(sql);
		Query(sql);
	}

	void Attach(const string &path = "far.example.com", const string &alias = "far") {
		Query("ATTACH '" + path + "' AS " + alias + " (TYPE fardb)");
	}

	void Seed() {
		Far("CREATE TABLE orders(id INTEGER PRIMARY KEY, name VARCHAR, amt INTEGER, tenant INTEGER, score DOUBLE, ts "
		    "TIMESTAMP)");
		Far("INSERT INTO orders VALUES (1, 'alpha', 50, 1, 1.5, '2024-01-01 00:00:00'), (2, 'beta', 150, 1, 2.5, "
		    "'2024-01-02 00:00:00'), (3, 'gamma', 250, 2, 3.5, '2024-01-03 00:00:00'), (4, NULL, 350, 2, NULL, NULL), "
		    "(5, 'epsilon', 450, 3, 5.5, '2024-01-05 00:00:00')");
		Far("CREATE TABLE customers(tenant INTEGER PRIMARY KEY, cname VARCHAR)");
		Far("INSERT INTO customers VALUES (1, 'acme'), (2, 'globex'), (4, 'initech')");
		Far("CREATE TABLE sales(region VARCHAR, quarter VARCHAR, amount INTEGER)");
		Far("INSERT INTO sales VALUES ('east', 'q1', 10), ('east', 'q2', 20), ('west', 'q1', 5), ('west', 'q3', 7)");
		Far("CREATE TABLE archive(id INTEGER PRIMARY KEY, name VARCHAR, amt INTEGER, tenant INTEGER, score DOUBLE, ts "
		    "TIMESTAMP)");
		Far("INSERT INTO archive VALUES (9, 'zeta', 900, 9, 9.5, '2024-09-09 00:00:00')");
		Far("CREATE TABLE types_t(b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT, h HUGEINT, f FLOAT, d "
		    "DOUBLE, dec DECIMAL(10,2), s VARCHAR, bl BLOB, dt DATE, tm TIME, ts TIMESTAMP, tstz TIMESTAMPTZ, iv "
		    "INTERVAL, l INTEGER[], u UUID)");
		Far("INSERT INTO types_t VALUES (true, 1, 2, 3, 4, 5, 1.5, 2.5, 3.25, 'x', '\\x00\\x01'::BLOB, '2024-01-01', "
		    "'12:34:56', '2024-01-01 12:34:56', '2024-01-01 12:34:56+00', INTERVAL 1 DAY, [1, 2], "
		    "'00000000-0000-0000-0000-000000000001')");
		Attach();
	}

	unique_ptr<MaterializedQueryResult> Query(const string &sql) {
		auto result = con.Query(sql);
		store->JoinArrivals();
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		return result;
	}

	string Error(const string &sql) {
		auto result = con.Query(sql);
		store->JoinArrivals();
		if (!result->HasError()) {
			FAIL("expected an error from: " + sql);
		}
		return result->GetError();
	}

	static string Unprefixed(string sql) {
		return StringUtil::Replace(StringUtil::Replace(sql, "far.", ""), "b.", "");
	}

	void Same(const string &near_sql) {
		Same(near_sql, Unprefixed(near_sql));
	}

	void Same(const string &near_sql, const string &far_sql) {
		INFO("near: " << near_sql);
		INFO("far:  " << far_sql);
		auto expected = store->con.Query(far_sql);
		if (expected->HasError()) {
			FAIL(expected->GetError());
		}
		auto got = Query(near_sql);
		REQUIRE(got->types == expected->types);
		vector<string> got_names;
		for (auto &name : got->names) {
			got_names.push_back(Unprefixed(name));
		}
		REQUIRE(got_names == expected->names);
		REQUIRE(got->RowCount() == expected->RowCount());
		auto want = Rows(*expected);
		auto have = Rows(*got);
		if (StringUtil::Upper(near_sql).find("ORDER BY") == string::npos) {
			std::sort(want.begin(), want.end(), RowLess);
			std::sort(have.begin(), have.end(), RowLess);
		}
		for (idx_t r = 0; r < want.size(); r++) {
			for (idx_t c = 0; c < want[r].size(); c++) {
				INFO("row " << r << " col " << c << ": have " << have[r][c].ToString() << " want "
				            << want[r][c].ToString());
				REQUIRE(ValueOperations::NotDistinctFrom(have[r][c], want[r][c]));
			}
		}
	}

	const FarCall &LastRead() {
		return store->LastRead();
	}

	const FarCall &LastWrite() {
		return store->LastWrite();
	}

private:
	static vector<vector<Value>> Rows(MaterializedQueryResult &result) {
		vector<vector<Value>> out;
		for (idx_t r = 0; r < result.RowCount(); r++) {
			vector<Value> row;
			for (idx_t c = 0; c < result.ColumnCount(); c++) {
				row.push_back(result.GetValue(c, r));
			}
			out.push_back(std::move(row));
		}
		return out;
	}

	static bool RowLess(const vector<Value> &a, const vector<Value> &b) {
		for (idx_t i = 0; i < a.size() && i < b.size(); i++) {
			if (ValueOperations::NotDistinctFrom(a[i], b[i])) {
				continue;
			}
			return ValueOperations::DistinctLessThan(a[i], b[i]);
		}
		return a.size() < b.size();
	}
};

struct Pair {
	shared_ptr<FarStore> far;
	shared_ptr<FarStore> other;
	DuckDB near;
	Connection con;

	Pair(Transport transport, bool other_first)
	    : far(make_shared_ptr<FarStore>(transport)), other(make_shared_ptr<FarStore>(transport)), near(nullptr),
	      con(near) {
		ExtensionLoader far_loader(*near.instance, "fardb");
		ExtensionLoader other_loader(*near.instance, "otherdb");
		if (other_first) {
			other::OtherSource::Register(other_loader, other);
			FarSource::Register(far_loader, far);
		} else {
			FarSource::Register(far_loader, far);
			other::OtherSource::Register(other_loader, other);
		}
	}

	~Pair() {
		far->JoinArrivals();
		other->JoinArrivals();
	}

	void Seed() {
		Store(*far, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
		Store(*far, "INSERT INTO t VALUES (1, 10), (2, 20)");
		Store(*other, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
		Store(*other, "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)");
		Query("ATTACH 'far.example.com' AS far (TYPE fardb)");
		Query("ATTACH 'other.example.com' AS other (TYPE otherdb)");
	}

	unique_ptr<MaterializedQueryResult> Query(const string &sql) {
		auto result = con.Query(sql);
		far->JoinArrivals();
		other->JoinArrivals();
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		return result;
	}

	static Value Scalar(FarStore &store, const string &sql) {
		auto result = store.con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
		return result->GetValue(0, 0);
	}

private:
	static void Store(FarStore &store, const string &sql) {
		auto result = store.con.Query(sql);
		if (result->HasError()) {
			FAIL(result->GetError());
		}
	}
};

} // namespace duckdb
