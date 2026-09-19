#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSession {
	CrossingWriter Write(ClientContext &) {
		return {};
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SESSION(duckdb::Bad);
