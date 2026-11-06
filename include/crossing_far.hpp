#pragma once

#include "duckdb/main/connection.hpp"

#include "crossing_link.hpp"
#include "crossing_substrait.hpp"

namespace duckdb {

struct CrossingFarOptions {
	//! What stands for a table a plan reads, such as a subquery with a row filter.
	CrossingFloorResolver floors;
	//! Called with each catalog a plan names; throws to refuse the plan.
	SubstraitCatalogCheck catalogs;
	//! `connection` already holds the transaction.
	bool transaction_open = false;
};

//! The far side of a link: everything but moving its frames. Serve holds one transaction on `connection`
//! until the near side ends it or closes the link, and returns then.
class CrossingFar {
public:
	CrossingFar(Connection &connection, string catalog, CrossingFarOptions options = CrossingFarOptions());

	void Serve(CrossingLink &link);

private:
	void RunStatement(CrossingLink &link, CrossingStream &control, const string &plan);

	Connection &connection;
	string catalog;
	CrossingFarOptions options;
};

} // namespace duckdb
