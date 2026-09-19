#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSource {
	vector<string> Tables(int) {
		return {};
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
