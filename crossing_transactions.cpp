#include "internal/crossing_transactions.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

CrossingTransactionManager::CrossingTransactionManager(AttachedDatabase &db, CrossingAttach &attach_p)
    : TransactionManager(db), attach(attach_p) {
}

Transaction &CrossingTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<Transaction>(*this, context);
	auto &raw = *transaction;
	lock_guard<mutex> guard(lock);
	live[&raw] = std::move(transaction);
	return raw;
}

void CrossingTransactionManager::Forget(Transaction &transaction) {
	lock_guard<mutex> guard(lock);
	live.erase(&transaction);
}

ErrorData CrossingTransactionManager::CommitTransaction(ClientContext &, Transaction &transaction) {
	auto error = attach.Commit(transaction);
	Forget(transaction);
	return error;
}

void CrossingTransactionManager::RollbackTransaction(Transaction &transaction) {
	attach.Rollback(transaction);
	Forget(transaction);
}

void CrossingTransactionManager::Checkpoint(ClientContext &, bool) {
}

} // namespace duckdb
