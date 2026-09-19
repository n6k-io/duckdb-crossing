#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSource {
	CrossingVerdict AcceptsCall(int) {
		return {};
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
