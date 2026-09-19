#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing.hpp"
#include "internal/fragment.hpp"

namespace duckdb {

class CrossingCarrier {
public:
	virtual ~CrossingCarrier() {
	}

	virtual const CrossingIdentity &Identity() const = 0;
	virtual CrossingVerb Verb() const = 0;
	virtual optional_ptr<CrossingFragment> Fragment() = 0;
	virtual CrossingSource &Source() = 0;
};

optional_ptr<CrossingCarrier> CarrierOf(LogicalOperator &op, const CrossingIdentity &identity);

optional_ptr<CrossingFragment> CrossingReadFragmentOf(LogicalOperator &op, const CrossingIdentity &identity);

optional_ptr<CrossingFragment> CrossingWriteFragmentOf(LogicalOperator &op, const CrossingIdentity &identity);

optional_ptr<CrossingSource> CrossingSourceOf(LogicalOperator &op, const CrossingIdentity &identity);

} // namespace duckdb
