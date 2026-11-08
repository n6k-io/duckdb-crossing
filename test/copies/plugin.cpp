#include "far_source/far_source.hpp"

#include "duckdb/main/extension/extension_loader.hpp"

namespace {

duckdb::shared_ptr<duckdb::FarStore> &Store() {
	static duckdb::shared_ptr<duckdb::FarStore> store = duckdb::make_shared_ptr<duckdb::FarStore>();
	return store;
}

} // namespace

extern "C" {

void crossing_copy_register(duckdb::DatabaseInstance *db) {
	duckdb::ExtensionLoader loader(*db, CROSSING_COPY_TYPE);
	CROSSING_COPY_SOURCE::Register(loader, Store());
}

//! Empty on success, else the error.
const char *crossing_copy_store_query(const char *sql) {
	static std::string error;
	auto result = Store()->con.Query(sql);
	error = result->HasError() ? result->GetError() : std::string();
	return error.c_str();
}

int64_t crossing_copy_store_scalar(const char *sql) {
	return Store()->con.Query(sql)->Fetch()->GetValue(0, 0).GetValue<int64_t>();
}
}
