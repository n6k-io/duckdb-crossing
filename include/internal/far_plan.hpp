#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include "crossing_link.hpp"
#include "internal/seam_inbox.hpp"
#include "internal/stream_sink.hpp"

namespace duckdb {

static constexpr const char *CROSSING_SEAM_ROWS_FUNCTION = "crossing_seam_rows";
static constexpr const char *CROSSING_SEAM_ORDERED_ROWS_FUNCTION = "crossing_seam_ordered_rows";

//! The seams of the statement `context` is running on far, by id.
struct CrossingFarSeams : public ClientContextState {
	static constexpr const char *KEY = "crossing_far_seams";

	map<idx_t, shared_ptr<CrossingSeamInbox>> inboxes;

	static CrossingFarSeams &Of(ClientContext &context);
};

void RegisterCrossingFarFunctions(DatabaseInstance &db);

struct CrossingFarSlice {
	idx_t id = 0;
	//! Near keeps the rows in the order the stream's batches give.
	bool ordered = false;
	unique_ptr<LogicalOperator> plan;
};

//! One plan of the statement's slices, each ending in a stream. It produces no rows.
unique_ptr<LogicalOperator> MakeFarRoot(Binder &binder, CrossingLink &link, vector<CrossingFarSlice> slices,
                                        shared_ptr<CrossingFailure> failure);

} // namespace duckdb
