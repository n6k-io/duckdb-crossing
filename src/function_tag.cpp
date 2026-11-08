#include "internal/function_tag.hpp"

namespace duckdb {

namespace {

const shared_ptr<TableFunctionInfo> &Tag() {
	static const shared_ptr<TableFunctionInfo> tag = make_shared_ptr<TableFunctionInfo>();
	return tag;
}

} // namespace

TableFunction &TagCrossingFunction(TableFunction &function) {
	function.function_info = Tag();
	return function;
}

bool IsOwnCrossingFunction(const LogicalGet &get) {
	return get.function.function_info == Tag();
}

} // namespace duckdb
