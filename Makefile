PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
-include $(PROJ_DIR)local.mk
DUCKDB_SOURCE_DIR ?= $(PROJ_DIR)duckdb
DUCKDB_BUILD_DIR ?= $(PROJ_DIR)build/duckdb
BUILD_DIR := $(PROJ_DIR)build/crossing
TEST_ARGS ?=
JOBS ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)

DUCKDB_LIBS := $(DUCKDB_BUILD_DIR)/src/libduckdb_static.a \
	$(DUCKDB_BUILD_DIR)/extension/libduckdb_generated_extension_loader.a \
	$(DUCKDB_BUILD_DIR)/extension/core_functions/libcore_functions_extension.a

DUCKDB_FIRST_LIB := $(firstword $(DUCKDB_LIBS))

duckdb: $(DUCKDB_LIBS)

$(DUCKDB_FIRST_LIB):
	@cmake -S "$(DUCKDB_SOURCE_DIR)" -B "$(DUCKDB_BUILD_DIR)" \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_UNITTESTS=OFF \
		-DBUILD_SHELL=OFF \
		-DSKIP_EXTENSIONS=parquet
	@cmake --build "$(DUCKDB_BUILD_DIR)" -j$(JOBS) --target duckdb_static duckdb_generated_extension_loader core_functions_extension

$(filter-out $(DUCKDB_FIRST_LIB),$(DUCKDB_LIBS)): $(DUCKDB_FIRST_LIB)

build: duckdb
	@cmake -S "$(PROJ_DIR)" -B "$(BUILD_DIR)" \
		-DDUCKDB_SOURCE_DIR="$(DUCKDB_SOURCE_DIR)" \
		-DDUCKDB_BUILD_DIR="$(DUCKDB_BUILD_DIR)"
	@cmake --build "$(BUILD_DIR)" -j$(JOBS)

test: build
	@"$(BUILD_DIR)/crossing_unittest" $(TEST_ARGS)
	@"$(BUILD_DIR)/crossing_library_unittest" $(TEST_ARGS)
	@ctest --test-dir "$(BUILD_DIR)" -R contract_ -j$(JOBS) --output-on-failure

clean:
	@rm -rf "$(PROJ_DIR)build"

.PHONY: duckdb build test clean

CLANG_FORMAT ?= $(PROJ_DIR).venv/bin/clang-format

$(CLANG_FORMAT): $(PROJ_DIR)pyproject.toml
	@uv sync --project "$(PROJ_DIR)"
FORMAT_FILES := $(wildcard $(PROJ_DIR)include/*.hpp $(PROJ_DIR)include/*/*.hpp $(PROJ_DIR)include/*/*.ipp) \
	$(shell find $(PROJ_DIR)src $(PROJ_DIR)test -name '*.cpp' -o -name '*.hpp')

format-check: $(CLANG_FORMAT)
	@$(CLANG_FORMAT) --dry-run --Werror $(FORMAT_FILES)

format-fix: $(CLANG_FORMAT)
	@$(CLANG_FORMAT) -i $(FORMAT_FILES)

.PHONY: format-check format-fix
