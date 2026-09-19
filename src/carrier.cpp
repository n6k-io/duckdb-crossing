#include "internal/carrier.hpp"

#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

optional_ptr<CrossingCarrier> CarrierOf(LogicalOperator &op, const CrossingIdentity &identity) {
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (!get.bind_data) {
		return nullptr;
	}
	auto carrier = dynamic_cast<CrossingCarrier *>(get.bind_data.get());
	if (!carrier || &carrier->Identity() != &identity) {
		return nullptr;
	}
	return carrier;
}

optional_ptr<CrossingFragment> CrossingReadFragmentOf(LogicalOperator &op, const CrossingIdentity &identity) {
	auto carrier = CarrierOf(op, identity);
	if (!carrier || carrier->Verb() != CrossingVerb::SELECT) {
		return nullptr;
	}
	auto &get = op.Cast<LogicalGet>();
	if (!get.table_filters.filters.empty() || get.dynamic_filters || !get.projected_input.empty() ||
	    get.ordinality_idx.IsValid()) {
		return nullptr;
	}
	return carrier->Fragment();
}

optional_ptr<CrossingFragment> CrossingWriteFragmentOf(LogicalOperator &op, const CrossingIdentity &identity) {
	auto carrier = CarrierOf(op, identity);
	if (!carrier || carrier->Verb() == CrossingVerb::SELECT) {
		return nullptr;
	}
	return carrier->Fragment();
}

optional_ptr<CrossingSource> CrossingSourceOf(LogicalOperator &op, const CrossingIdentity &identity) {
	if (CrossingReadFragmentOf(op, identity) || CrossingWriteFragmentOf(op, identity)) {
		return &CarrierOf(op, identity)->Source();
	}
	return nullptr;
}

} // namespace duckdb
