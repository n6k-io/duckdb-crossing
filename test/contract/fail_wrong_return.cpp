#include "contract_case.hpp"

namespace duckdb {

struct Bad : GoodSource {
	int Describe(const string &, const string &) {
		return 0;
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
