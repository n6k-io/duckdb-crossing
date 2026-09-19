#include "contract_case.hpp"

namespace duckdb {

struct Bad final : GoodSource {};

} // namespace duckdb

CROSSING_CONTRACT_SOURCE(duckdb::Bad);
