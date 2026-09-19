#include "contract_case.hpp"

namespace duckdb {

struct Bad {
	void Commit() {
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SESSION(duckdb::Bad);
