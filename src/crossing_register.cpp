#include "crossing.hpp"
#include "internal/crossing_catalog.hpp"
#include "internal/crossing_pass.hpp"
#include "internal/crossing_transactions.hpp"
#include "internal/far_plan.hpp"
#include "internal/fill.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//! The factory reaches attach through here: StorageExtension::attach is a plain function pointer,
//! so a capturing lambda cannot be one.
struct CrossingStorageInfo : public StorageExtensionInfo {
	CrossingStorageInfo(string type_p, CrossingSourceFactory factory_p)
	    : type(std::move(type_p)), factory(std::move(factory_p)) {
	}

	string type;
	CrossingSourceFactory factory;
};

unique_ptr<Catalog> AttachCrossingCatalog(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &, AttachInfo &info, AttachOptions &) {
	auto &crossing_info = static_cast<CrossingStorageInfo &>(*storage_info);
	auto source = crossing_info.factory(context, info);
	if (!source) {
		throw IOException("%s: the factory for TYPE %s returned no source for '%s'", crossing_info.type,
		                  crossing_info.type, info.path);
	}
	info.path = string();
	return make_uniq<CrossingCatalog>(db, crossing_info.type, std::move(source));
}

unique_ptr<TransactionManager> CreateCrossingTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<CrossingTransactionManager>(db, CrossingAttach::Of(catalog));
}

//! Weak, not raw: a live weak_ptr keeps the info's make_shared block allocated, so a freed info's
//! address cannot come back as another copy's.
struct OwnPasses {
	mutex lock;
	vector<weak_ptr<CrossingPassInfo>> infos;

	optional_ptr<CrossingPassInfo> Find(const OptimizerExtensionInfo *info) {
		for (auto &weak : infos) {
			auto own = weak.lock();
			if (own && own.get() == info) {
				return own.get();
			}
		}
		return nullptr;
	}
};

OwnPasses &Passes() {
	static OwnPasses passes;
	return passes;
}

} // namespace

void RegisterCrossingPass(DatabaseInstance &db, const CrossingIdentity &identity) {
	auto &config = DBConfig::GetConfig(db);
	auto &passes = Passes();
	lock_guard<mutex> guard(passes.lock);
	for (auto &existing : config.GetCallbackManager().OptimizerExtensions()) {
		auto own = passes.Find(existing.optimizer_info.get());
		if (own && &own->identity == &identity) {
			return;
		}
	}
	passes.infos.erase(std::remove_if(passes.infos.begin(), passes.infos.end(),
	                                  [](const weak_ptr<CrossingPassInfo> &weak) { return weak.expired(); }),
	                   passes.infos.end());
	OptimizerExtension pass;
	pass.pre_optimize_function = CrossingFoldPass;
	pass.optimize_function = CrossingNarrowPass;
	auto info = make_shared_ptr<CrossingPassInfo>(identity);
	passes.infos.push_back(info);
	pass.optimizer_info = std::move(info);
	OptimizerExtension::Register(config, pass);
	RegisterSourceDefaultFunction(db);
}

void RegisterCrossing(ExtensionLoader &loader, const string &type, CrossingSourceFactory factory,
                      const CrossingIdentity &identity) {
	auto ext = make_shared_ptr<StorageExtension>();
	ext->attach = AttachCrossingCatalog;
	ext->create_transaction_manager = CreateCrossingTransactionManager;
	ext->storage_info = make_uniq<CrossingStorageInfo>(type, std::move(factory));

	auto &db = loader.GetDatabaseInstance();
	StorageExtension::Register(DBConfig::GetConfig(db), type, std::move(ext));
	RegisterCrossingPass(db, identity);
	RegisterCrossingFarFunctions(db);
}

} // namespace duckdb
