cmake_minimum_required(VERSION 3.5)

project(crossing)

include(${CMAKE_CURRENT_SOURCE_DIR}/crossing.cmake)

if(NOT CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  return()
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(DUCKDB_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/duckdb"
    CACHE PATH "Directory of the duckdb source tree")
set(DUCKDB_BUILD_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/build/duckdb"
    CACHE PATH "Directory of a configured duckdb build, for libduckdb_static.a")

set(DUCKDB_STATIC_LIB ${DUCKDB_BUILD_DIR}/src/libduckdb_static.a)
set(DUCKDB_CORE_FUNCTIONS_LIB
    ${DUCKDB_BUILD_DIR}/extension/core_functions/libcore_functions_extension.a)

foreach(lib ${DUCKDB_STATIC_LIB} ${DUCKDB_CORE_FUNCTIONS_LIB})
  if(NOT EXISTS ${lib})
    message(
      FATAL_ERROR "missing ${lib} -- set DUCKDB_BUILD_DIR to a built duckdb")
  endif()
endforeach()

crossing_engine_sources(CROSSING_ENGINE_SOURCES)
crossing_sources(CROSSING_LIBRARY_SOURCES)
crossing_includes(CROSSING_INCLUDES)

find_package(Threads REQUIRED)

set(LINK_EXTENSION_LIST core_functions)
set(DESCRIBE_DECLARATIONS
    "int32_t duckdb_extension_core_functions_describe(duckdb_extension_descriptor *descriptor);"
)
set(DESCRIBE_REGISTRATIONS
    "\tif (duckdb_register_static_extension(duckdb_extension_core_functions_describe) != 0) {\n\t\tresult = 1;\n\t}"
)
set(CROSSING_EXTENSION_LOADER
    ${CMAKE_CURRENT_BINARY_DIR}/crossing_static_extension_loader.c)
configure_file(
  ${DUCKDB_SOURCE_DIR}/extension/loader/static_extension_loader.c.in
  ${CROSSING_EXTENSION_LOADER} @ONLY)

function(crossing_test_target target)
  add_executable(
    ${target}
    test/unittest_main.cpp ${CROSSING_EXTENSION_LOADER}
    ${DUCKDB_SOURCE_DIR}/extension/loader/static_extension_autoregister.cpp
    ${ARGN})
  target_include_directories(
    ${target}
    PRIVATE ${DUCKDB_SOURCE_DIR}/src/include
            ${DUCKDB_SOURCE_DIR}/third_party/catch
            ${DUCKDB_SOURCE_DIR}/third_party/fmt/include
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CROSSING_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/test)
  if(APPLE)
    target_link_libraries(
      ${target} ${DUCKDB_CORE_FUNCTIONS_LIB} ${DUCKDB_STATIC_LIB}
      Threads::Threads)
  else()
    target_link_libraries(
      ${target} -Wl,--start-group ${DUCKDB_CORE_FUNCTIONS_LIB}
      ${DUCKDB_STATIC_LIB} -Wl,--end-group
      Threads::Threads ${CMAKE_DL_LIBS})
  endif()
endfunction()

file(GLOB CROSSING_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test/test_*.cpp)
crossing_test_target(crossing_unittest ${CROSSING_ENGINE_SOURCES}
                     ${CROSSING_TEST_SOURCES})

crossing_substrait_includes(CROSSING_SUBSTRAIT_INCLUDES)
file(GLOB CROSSING_LIBRARY_TEST_SOURCES CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/test/library/test_*.cpp)
crossing_test_target(
  crossing_library_unittest ${CROSSING_LIBRARY_SOURCES}
  test/far_source/far_source.cpp
  test/own_catalog/own_catalog.cpp
  ${CROSSING_LIBRARY_TEST_SOURCES})
target_include_directories(
  crossing_library_unittest
  PRIVATE ${CROSSING_SUBSTRAIT_INCLUDES}
          ${DUCKDB_SOURCE_DIR}/third_party/yyjson/include)
target_link_libraries(crossing_library_unittest
                      ${DUCKDB_BUILD_DIR}/third_party/yyjson/libduckdb_yyjson.a)

function(crossing_copy_plugin target type source)
  add_library(${target} MODULE ${CROSSING_LIBRARY_SOURCES}
                               test/far_source/far_source.cpp test/copies/plugin.cpp)
  target_compile_definitions(${target} PRIVATE CROSSING_COPY_TYPE="${type}"
                                               CROSSING_COPY_SOURCE=${source})
  target_include_directories(
    ${target}
    PRIVATE ${DUCKDB_SOURCE_DIR}/src/include
            ${DUCKDB_SOURCE_DIR}/third_party/catch
            ${DUCKDB_SOURCE_DIR}/third_party/fmt/include
            ${DUCKDB_SOURCE_DIR}/third_party/yyjson/include
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${CROSSING_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/test
            ${CROSSING_SUBSTRAIT_INCLUDES})
  target_link_libraries(${target}
                        ${DUCKDB_BUILD_DIR}/third_party/yyjson/libduckdb_yyjson.a)
  if(APPLE)
    target_link_options(${target} PRIVATE -undefined dynamic_lookup)
  endif()
endfunction()

crossing_copy_plugin(crossing_copy_far fardb duckdb::FarSource)
crossing_copy_plugin(crossing_copy_other otherdb other::OtherSource)

add_executable(crossing_copies_unittest test/unittest_main.cpp
                                        test/copies/test_copies.cpp)
target_include_directories(
  crossing_copies_unittest PRIVATE ${DUCKDB_SOURCE_DIR}/src/include
                                   ${DUCKDB_SOURCE_DIR}/third_party/catch)
target_compile_definitions(
  crossing_copies_unittest
  PRIVATE CROSSING_COPY_FAR="$<TARGET_FILE:crossing_copy_far>"
          CROSSING_COPY_OTHER="$<TARGET_FILE:crossing_copy_other>")
add_dependencies(crossing_copies_unittest crossing_copy_far crossing_copy_other)
set_target_properties(crossing_copies_unittest PROPERTIES ENABLE_EXPORTS ON)
if(APPLE)
  target_link_libraries(crossing_copies_unittest -Wl,-force_load,${DUCKDB_STATIC_LIB}
                        Threads::Threads)
else()
  target_link_libraries(
    crossing_copies_unittest -Wl,--whole-archive ${DUCKDB_STATIC_LIB}
    -Wl,--no-whole-archive Threads::Threads ${CMAKE_DL_LIBS})
endif()

enable_testing()
