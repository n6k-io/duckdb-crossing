cmake_minimum_required(VERSION 3.5)

project(crossing)

include(${CMAKE_CURRENT_SOURCE_DIR}/crossing.cmake)

if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  return()
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(DUCKDB_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/duckdb"
    CACHE PATH "Directory of the duckdb source tree")
set(DUCKDB_BUILD_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/build/duckdb"
    CACHE PATH "Directory of a configured duckdb build, for libduckdb_static.a")

set(DUCKDB_STATIC_LIB ${DUCKDB_BUILD_DIR}/src/libduckdb_static.a)
set(DUCKDB_LOADER_LIB
    ${DUCKDB_BUILD_DIR}/extension/libduckdb_generated_extension_loader.a)
set(DUCKDB_CORE_FUNCTIONS_LIB
    ${DUCKDB_BUILD_DIR}/extension/core_functions/libcore_functions_extension.a)

foreach(lib ${DUCKDB_STATIC_LIB} ${DUCKDB_LOADER_LIB}
            ${DUCKDB_CORE_FUNCTIONS_LIB})
  if(NOT EXISTS ${lib})
    message(
      FATAL_ERROR "missing ${lib} -- set DUCKDB_BUILD_DIR to a built duckdb")
  endif()
endforeach()

crossing_engine_sources(CROSSING_ENGINE_SOURCES)
crossing_sources(CROSSING_LIBRARY_SOURCES)
crossing_includes(CROSSING_INCLUDES)

find_package(Threads REQUIRED)

function(crossing_test_target target)
  add_executable(${target} test/unittest_main.cpp ${ARGN})
  target_include_directories(
    ${target}
    PRIVATE ${DUCKDB_SOURCE_DIR}/src/include
            ${DUCKDB_SOURCE_DIR}/third_party/catch ${CMAKE_CURRENT_SOURCE_DIR}
            ${CROSSING_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/test)
  if(APPLE)
    target_link_libraries(
      ${target} ${DUCKDB_LOADER_LIB} ${DUCKDB_CORE_FUNCTIONS_LIB}
      ${DUCKDB_STATIC_LIB} Threads::Threads)
  else()
    target_link_libraries(
      ${target} -Wl,--start-group ${DUCKDB_LOADER_LIB}
      ${DUCKDB_CORE_FUNCTIONS_LIB} ${DUCKDB_STATIC_LIB} -Wl,--end-group
      Threads::Threads ${CMAKE_DL_LIBS})
  endif()
endfunction()

file(GLOB CROSSING_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test/test_*.cpp)
crossing_test_target(crossing_unittest ${CROSSING_ENGINE_SOURCES}
                     ${CROSSING_TEST_SOURCES})

crossing_substrait_sources(CROSSING_SUBSTRAIT_SOURCES)
crossing_substrait_includes(CROSSING_SUBSTRAIT_INCLUDES)
file(GLOB CROSSING_LIBRARY_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test/library/test_*.cpp)
crossing_test_target(
  crossing_library_unittest ${CROSSING_LIBRARY_SOURCES}
  ${CROSSING_SUBSTRAIT_SOURCES} test/far_source/far_source.cpp
  test/read_only_source/read_only_source.cpp test/own_catalog/own_catalog.cpp
  ${CROSSING_LIBRARY_TEST_SOURCES})
target_include_directories(
  crossing_library_unittest
  PRIVATE ${CROSSING_SUBSTRAIT_INCLUDES}
          ${DUCKDB_SOURCE_DIR}/third_party/yyjson/include)
target_link_libraries(crossing_library_unittest
                      ${DUCKDB_BUILD_DIR}/third_party/yyjson/libduckdb_yyjson.a)

enable_testing()

function(crossing_contract_case name)
  add_library(contract_${name} OBJECT EXCLUDE_FROM_ALL
              test/contract/${name}.cpp)
  target_include_directories(
    contract_${name} PRIVATE ${DUCKDB_SOURCE_DIR}/src/include
                             ${CROSSING_INCLUDES})
endfunction()

function(crossing_contract_ok name)
  crossing_contract_case(${name})
  set_target_properties(contract_${name} PROPERTIES EXCLUDE_FROM_ALL FALSE)
endfunction()

function(crossing_contract_fail name expected)
  crossing_contract_case(${name})
  add_test(NAME contract_${name}
           COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target
                   contract_${name})
  set_tests_properties(contract_${name} PROPERTIES PASS_REGULAR_EXPRESSION
                                                   "${expected}")
endfunction()

crossing_contract_ok(ok_minimal)
crossing_contract_ok(ok_optional)
crossing_contract_fail(fail_no_session_type "needs `using Session")
crossing_contract_fail(fail_missing_tables
                       "missing required member `vector<string> Tables")
crossing_contract_fail(fail_wrong_parameters
                       "`Tables` exists but is not callable")
crossing_contract_fail(fail_private_member
                       "`Tables` exists but is not callable")
crossing_contract_fail(fail_wrong_return
                       "ReturnCheck<false, duckdb::CrossingTable, int>")
crossing_contract_fail(
  fail_begin_shared_ptr
  "ReturnCheck<false, duckdb::unique_ptr<duckdb::GoodSession.*duckdb::shared_ptr<duckdb::GoodSession"
)
crossing_contract_fail(fail_final_source "source type must not be final")
crossing_contract_fail(fail_optional_wrong_signature
                       "`AcceptsCall` exists but is not callable")
crossing_contract_fail(fail_session_missing_read
                       "missing required member `CrossingScan Read")
crossing_contract_fail(fail_session_commit_returns
                       "ReturnCheck<false, void, int>")
crossing_contract_fail(fail_session_write_signature
                       "`Write` exists but is not callable")
