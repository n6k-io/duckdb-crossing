#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

#include "crossing_attach.hpp"

namespace duckdb {

class CrossingTransactionManager : public TransactionManager {
public:
	CrossingTransactionManager(AttachedDatabase &db, CrossingAttach &attach);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force) override;

private:
	void Forget(Transaction &transaction);

	CrossingAttach &attach;
	mutex lock;
	unordered_map<Transaction *, unique_ptr<Transaction>> live;
};

} // namespace duckdb
