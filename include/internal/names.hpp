#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

inline vector<string> NamesOf(const vector<Identifier> &identifiers) {
	vector<string> out;
	out.reserve(identifiers.size());
	for (auto &identifier : identifiers) {
		out.push_back(identifier.GetIdentifierName());
	}
	return out;
}

inline vector<Identifier> IdentifiersOf(const vector<string> &names) {
	vector<Identifier> out;
	out.reserve(names.size());
	for (auto &name : names) {
		out.emplace_back(name);
	}
	return out;
}

} // namespace duckdb
