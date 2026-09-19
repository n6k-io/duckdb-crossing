#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSession {
	void Ddl(const CrossingDdl &) {
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SESSION(duckdb::Bad);
