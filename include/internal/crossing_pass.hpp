#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"

namespace duckdb {

struct CrossingPassInfo : public OptimizerExtensionInfo {
	explicit CrossingPassInfo(const CrossingIdentity &identity_p) : identity(identity_p) {
	}

	const CrossingIdentity &identity;
};

void CrossingFoldPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

void CrossingNarrowPass(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
