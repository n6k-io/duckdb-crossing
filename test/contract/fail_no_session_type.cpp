#include "contract_case.hpp"

namespace duckdb {

struct Bad {
	vector<string> Tables(const string &) {
		return {};
	}
	CrossingTable Describe(const string &, const string &) {
		return {};
	}
	CrossingPlan Plan(const CrossingPlanRequest &) {
		return {};
	}
	unique_ptr<GoodSession> Begin(ClientContext &) {
		return nullptr;
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
