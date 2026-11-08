#include "catch.hpp"

#include "duckdb.hpp"

#include <dlfcn.h>

using namespace duckdb;

namespace {

//! One plugin: its own compiled copy of crossing, loaded the way DuckDB loads an extension.
struct Copy {
	void *handle;
	void (*register_)(DatabaseInstance *);
	const char *(*store_query)(const char *);
	int64_t (*store_scalar)(const char *);

	explicit Copy(const char *path) : handle(dlopen(path, RTLD_NOW | RTLD_LOCAL)) {
		if (!handle) {
			FAIL(dlerror());
		}
		register_ = reinterpret_cast<void (*)(DatabaseInstance *)>(dlsym(handle, "crossing_copy_register"));
		store_query = reinterpret_cast<const char *(*)(const char *)>(dlsym(handle, "crossing_copy_store_query"));
		store_scalar = reinterpret_cast<int64_t (*)(const char *)>(dlsym(handle, "crossing_copy_store_scalar"));
		REQUIRE(register_);
		REQUIRE(store_query);
		REQUIRE(store_scalar);
	}

	void Store(const string &sql) {
		string error = store_query(sql.c_str());
		if (!error.empty()) {
			FAIL(sql + "\n" + error);
		}
	}
};

Copy &FarCopy() {
	static Copy copy(CROSSING_COPY_FAR);
	return copy;
}

Copy &OtherCopy() {
	static Copy copy(CROSSING_COPY_OTHER);
	return copy;
}

struct Copies {
	DuckDB near;
	Connection con;

	Copies() : near(nullptr), con(near) {
		auto &far = FarCopy();
		auto &other = OtherCopy();
		far.Store("DROP TABLE IF EXISTS t");
		far.Store("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
		far.Store("INSERT INTO t VALUES (1, 10), (2, 20)");
		other.Store("DROP TABLE IF EXISTS t");
		other.Store("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER)");
		other.Store("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)");
		far.register_(near.instance.get());
		other.register_(near.instance.get());
		Query("ATTACH 'far.example.com' AS far (TYPE fardb)");
		Query("ATTACH 'other.example.com' AS other (TYPE otherdb)");
	}

	Value Query(const string &sql) {
		auto result = con.Query(sql);
		if (result->HasError()) {
			FAIL(sql + "\n" + result->GetError());
		}
		auto chunk = result->Fetch();
		return chunk && chunk->size() ? chunk->GetValue(0, 0) : Value();
	}
};

} // namespace

TEST_CASE("two copies of crossing in one process each read only their own tables", "[copies]") {
	Copies copies;
	REQUIRE(copies.Query("SELECT count(*) FROM far.t") == Value::BIGINT(2));
	REQUIRE(copies.Query("SELECT count(*) FROM other.t") == Value::BIGINT(3));
	REQUIRE(copies.Query("SELECT o.v - f.v FROM far.t f JOIN other.t o USING (id) WHERE id = 2") ==
	        Value::INTEGER(180));
}

TEST_CASE("two copies of crossing in one process each write only their own tables", "[copies]") {
	Copies copies;
	copies.Query("UPDATE other.t SET v = 0 WHERE id = 1");
	copies.Query("UPDATE far.t SET v = 0 WHERE id = 2");
	copies.Query("DELETE FROM other.t WHERE id = 2");
	copies.Query("INSERT INTO far.t VALUES (9, 90)");
	copies.Query("INSERT INTO far.t SELECT id, v FROM other.t WHERE id = 3");
	copies.Query("INSERT INTO other.t SELECT id + 10, v FROM far.t WHERE id = 9");

	auto &far = FarCopy();
	auto &other = OtherCopy();
	REQUIRE(far.store_scalar("SELECT count(*) FROM t") == 4);
	REQUIRE(far.store_scalar("SELECT v FROM t WHERE id = 2") == 0);
	REQUIRE(far.store_scalar("SELECT v FROM t WHERE id = 3") == 300);
	REQUIRE(other.store_scalar("SELECT count(*) FROM t") == 3);
	REQUIRE(other.store_scalar("SELECT v FROM t WHERE id = 1") == 0);
	REQUIRE(other.store_scalar("SELECT v FROM t WHERE id = 19") == 90);
}
