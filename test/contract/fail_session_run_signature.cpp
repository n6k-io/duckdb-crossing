#include "contract_case.hpp"

namespace duckdb {

struct Bad {
	CrossingScan Run(ClientContext &) {
		return {};
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SESSION(duckdb::Bad);
