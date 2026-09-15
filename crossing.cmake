set(CROSSING_ROOT ${CMAKE_CURRENT_LIST_DIR})

set(CROSSING_ENGINE_SOURCE_NAMES
    crossing.cpp
    fragment.cpp
    seam.cpp
    floor.cpp
    plan_wire.cpp
    source.cpp
    source_evaluation.cpp
    rules.cpp
    labelling.cpp
    fold.cpp
    filter_halves.cpp
    pass.cpp
    seam_split.cpp
    scan_columns.cpp
    table_indices.cpp)

set(CROSSING_LIBRARY_SOURCE_NAMES
    ${CROSSING_ENGINE_SOURCE_NAMES}
    crossing_attach.cpp
    crossing_register.cpp
    crossing_catalog.cpp
    crossing_table_entry.cpp
    crossing_scan.cpp
    crossing_read.cpp
    parking.cpp
    crossing_write.cpp
    crossing_shape.cpp
    crossing_pass.cpp
    crossing_transactions.cpp)

function(_crossing_expand out_var)
  set(result "")
  foreach(name ${ARGN})
    list(APPEND result ${CROSSING_ROOT}/${name})
  endforeach()
  set(${out_var}
      ${result}
      PARENT_SCOPE)
endfunction()

function(crossing_engine_sources out_var)
  _crossing_expand(result ${CROSSING_ENGINE_SOURCE_NAMES})
  set(${out_var}
      ${result}
      PARENT_SCOPE)
endfunction()

function(crossing_sources out_var)
  _crossing_expand(result ${CROSSING_LIBRARY_SOURCE_NAMES})
  set(${out_var}
      ${result}
      PARENT_SCOPE)
endfunction()

function(crossing_includes out_var)
  set(${out_var}
      ${CROSSING_ROOT}/include
      PARENT_SCOPE)
endfunction()

function(crossing_add_to_target target)
  crossing_sources(sources)
  target_sources(${target} PRIVATE ${sources})
  target_include_directories(${target} PRIVATE ${CROSSING_ROOT}/include)
endfunction()
