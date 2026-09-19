#include "contract_case.hpp"

namespace duckdb {

class Bad {
public:
	using Session = GoodSession;
	CrossingTable Describe(const string &, const string &) {
		return {};
	}
	CrossingPlan Plan(const CrossingPlanRequest &) {
		return {};
	}
	unique_ptr<Session> Begin(ClientContext &) {
		return nullptr;
	}

private:
	vector<string> Tables(const string &) {
		return {};
	}
};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
