#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSource {
	shared_ptr<Session> Begin(ClientContext &) {
		return nullptr;
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
