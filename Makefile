PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
DUCKDB_SOURCE_DIR := $(PROJ_DIR)duckdb
DUCKDB_BUILD_DIR := $(PROJ_DIR)build/duckdb
BUILD_DIR := $(PROJ_DIR)build/crossing
TEST_ARGS ?=
JOBS ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 2)

DUCKDB_LIBS := $(DUCKDB_BUILD_DIR)/src/libduckdb_static.a \
	$(DUCKDB_BUILD_DIR)/extension/libduckdb_generated_extension_loader.a \
	$(DUCKDB_BUILD_DIR)/extension/core_functions/libcore_functions_extension.a

duckdb: $(DUCKDB_LIBS)

$(DUCKDB_LIBS):
	@cmake -S "$(DUCKDB_SOURCE_DIR)" -B "$(DUCKDB_BUILD_DIR)" \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_UNITTESTS=OFF \
		-DBUILD_SHELL=OFF \
		-DSKIP_EXTENSIONS=parquet
	@cmake --build "$(DUCKDB_BUILD_DIR)" -j$(JOBS) --target duckdb_static duckdb_generated_extension_loader core_functions_extension

build: duckdb
	@cmake -S "$(PROJ_DIR)" -B "$(BUILD_DIR)" \
		-DDUCKDB_SOURCE_DIR="$(DUCKDB_SOURCE_DIR)" \
		-DDUCKDB_BUILD_DIR="$(DUCKDB_BUILD_DIR)"
	@cmake --build "$(BUILD_DIR)" -j$(JOBS)

test: build
	@"$(BUILD_DIR)/crossing_unittest" $(TEST_ARGS)
	@"$(BUILD_DIR)/crossing_library_unittest" $(TEST_ARGS)
	@"$(BUILD_DIR)/crossing_substrait_unittest" $(TEST_ARGS)

clean:
	@rm -rf "$(PROJ_DIR)build"

.PHONY: duckdb build test clean
