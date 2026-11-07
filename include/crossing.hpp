#pragma once

// The API a source implements. See IMPLEMENTING.md.
//
// Vocabulary:
//   source     the database a CrossingSource fronts; near is the DuckDB attaching it, far the DuckDB behind it
//   link       one far transaction's streams, which the source moves frames over
//   slice      the part of a statement's plan that runs on far
//   seam       where a slice reads rows near sends it
//   waker      what a stream that cannot go on yet calls when it can

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/constraint.hpp"

#include <functional>

namespace duckdb {

class ExtensionLoader;
class Expression;
class LogicalOperator;
class ClientContext;
class DatabaseInstance;
class TableRef;
class CrossingLink;
struct AttachInfo;

//! DELETE_ trails an underscore: DELETE is a macro in some Windows SDK headers.
enum class CrossingVerb : uint8_t { SELECT = 0, INSERT = 1, UPDATE = 2, DELETE_ = 3, CREATE = 4, ALTER = 5, DROP = 6 };

const char *CrossingVerbName(CrossingVerb verb);

const vector<CrossingVerb> &CrossingVerbs();

bool IsDdlVerb(CrossingVerb verb);

struct CrossingVerdict {
	bool ok = true;
	string reason;

	static CrossingVerdict Yes() {
		return CrossingVerdict();
	}
	static CrossingVerdict No(string reason) {
		CrossingVerdict verdict;
		verdict.ok = false;
		verdict.reason = std::move(reason);
		return verdict;
	}
};

struct CrossingTable {
	string name;
	//! Empty: the catalog far binds in. Set by a source whose group spans catalogs.
	string catalog;
	//! Source order. That order is what every column position in a query means.
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! A verb absent here is refused before the statement runs.
	vector<CrossingVerb> verbs;
	//! Whether a write may return rows. Without it RETURNING is refused at bind.
	bool returning = false;
	//! Whether far's table has a `rowid`, as a view has not. Without it a write that crossing cannot say as target,
	//! source and condition, such as one with a subquery in its WHERE, is refused at bind.
	bool row_ids = true;
	//! Constraints the source enforces. Declaring one your source does not enforce claims a
	//! guarantee nothing keeps, so only pass what is real.
	vector<unique_ptr<Constraint>> constraints;

	void Column(string column_name, LogicalType type) {
		column_names.push_back(std::move(column_name));
		column_types.push_back(std::move(type));
	}
	bool Allows(CrossingVerb verb) const {
		for (auto allowed : verbs) {
			if (allowed == verb) {
				return true;
			}
		}
		return false;
	}
};

struct CrossingSchema {
	string name;
	vector<CrossingVerb> verbs;

	bool Allows(CrossingVerb verb) const {
		for (auto allowed : verbs) {
			if (allowed == verb) {
				return true;
			}
		}
		return false;
	}
};

//! A table a far plan reads, for CrossingFarOptions::floors.
struct CrossingFloor {
	//! Empty: the catalog far binds in.
	string catalog;
	string schema;
	string table;
};

//! What stands for a table far reads: a TableRef producing its columns, in order, then its `rowid`. An UPDATE or
//! DELETE reaches only rows it produces, and a MERGE into a table it replaces is refused. Null means the table itself.
using CrossingFloorResolver = std::function<unique_ptr<TableRef>(const CrossingFloor &)>;

struct CrossingParking;

class CrossingWaker {
public:
	CrossingWaker() = default;

	void Wake() const;

private:
	friend struct CrossingParking;
	explicit CrossingWaker(shared_ptr<CrossingParking> parking_p) : parking(std::move(parking_p)) {
	}

	shared_ptr<CrossingParking> parking;
};

//! What tells one copy of crossing's nodes from another's.
struct CrossingIdentity {
	CrossingIdentity() = default;
	CrossingIdentity(const CrossingIdentity &) = delete;
	CrossingIdentity &operator=(const CrossingIdentity &) = delete;
};

const CrossingIdentity &CrossingDefaultIdentity();

class CrossingSource {
public:
	virtual ~CrossingSource() = default;

	virtual vector<string> Tables(const string &schema) = 0;
	virtual CrossingTable Describe(const string &schema, const string &name) = 0;
	//! One far transaction. Called when a transaction first needs far.
	virtual unique_ptr<CrossingLink> Connect(ClientContext &context) = 0;

	virtual vector<string> Schemas() {
		return {"main"};
	}
	virtual CrossingSchema DescribeSchema(const string &schema) {
		CrossingSchema result;
		result.name = schema;
		return result;
	}
	//! Whether far may run these. Not asked about casts, comparisons or BETWEEN; a lambda is refused without asking.
	virtual CrossingVerdict AcceptsCall(const Expression &expr) {
		return CrossingVerdict::No("this source computes nothing");
	}
	virtual CrossingVerdict AcceptsType(const LogicalType &type) {
		return CrossingVerdict::No("this source holds only what it was described with");
	}
	virtual CrossingVerdict AcceptsOperator(const LogicalOperator &op) {
		return CrossingVerdict::Yes();
	}
	//! Whether a join may run on far over rows near sends, with these column types.
	virtual CrossingVerdict AcceptsSeam(const vector<LogicalType> &types) {
		return CrossingVerdict::No("this source takes no rows into a read");
	}
	//! Sources with one group serve one store through one link, so a plan may join them. They must answer the
	//! Accepts* calls alike.
	virtual const void *Group() const {
		return this;
	}
	virtual void Detach(ClientContext &context) {
	}
	//! Fills in `table` when a transaction reads a schema it changed. Far has given its columns and its primary key,
	//! unique and not-null constraints; remove any you do not declare in Describe. Defaults to SELECT and DROP,
	//! plus INSERT, UPDATE, DELETE and ALTER when far's table has row ids, with RETURNING allowed.
	virtual void DescribeChanged(const string &schema, CrossingTable &table);
	//! Told of each slice as it is planned, before it is sent. For logging and tests.
	virtual void Planned(const LogicalOperator &plan) {
	}

	virtual const CrossingIdentity &Identity() const {
		return CrossingDefaultIdentity();
	}
};

using CrossingSourceFactory = std::function<unique_ptr<CrossingSource>(ClientContext &context, AttachInfo &info)>;

void RegisterCrossing(ExtensionLoader &loader, const string &type, CrossingSourceFactory factory,
                      const CrossingIdentity &identity = CrossingDefaultIdentity());
void RegisterCrossingPass(DatabaseInstance &db, const CrossingIdentity &identity = CrossingDefaultIdentity());

} // namespace duckdb
