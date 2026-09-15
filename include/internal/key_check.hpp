#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! `affected` exceeds `keys_sent` only when a key reached a row the scan did not name. The count is
//! reported, never the value: the value would say which hidden row exists.
inline void ThrowIfKeyNotUnique(idx_t affected, idx_t keys_sent, const string &table, const vector<string> &key_columns,
                                const char *verb) {
	if (affected <= keys_sent) {
		return;
	}
	throw ConstraintException("crossing: %s on '%s' matched %llu source rows for %llu key value(s); the declared key "
	                          "(%s) is not unique on the source. The write was already sent.",
	                          verb, table, static_cast<uint64_t>(affected), static_cast<uint64_t>(keys_sent),
	                          StringUtil::Join(key_columns, ", "));
}

} // namespace duckdb
