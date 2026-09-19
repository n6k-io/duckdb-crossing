#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSession {
	int Commit() {
		return 0;
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SESSION(duckdb::Bad);
