set(CROSSING_ROOT ${CMAKE_CURRENT_LIST_DIR})

set(CROSSING_ENGINE_SOURCE_NAMES
    crossing.cpp
    carrier.cpp
    fragment.cpp
    seam.cpp
    floor.cpp
    plan_wire.cpp
    source_evaluation.cpp
    rules.cpp
    labelling.cpp
    fold.cpp
    filter_halves.cpp
    fill.cpp
    pass.cpp
    scan_columns.cpp
    table_indices.cpp)

set(CROSSING_LIBRARY_SOURCE_NAMES
    ${CROSSING_ENGINE_SOURCE_NAMES}
    crossing_attach.cpp
    crossing_catalog.cpp
    crossing_table_entry.cpp
    crossing_read.cpp
    parking.cpp
    crossing_write.cpp
    crossing_fill.cpp
    crossing_pass.cpp
    crossing_transactions.cpp
    crossing_register.cpp)

function(_crossing_expand out_var)
  set(result "")
  foreach(name ${ARGN})
    list(APPEND result ${CROSSING_ROOT}/src/${name})
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

set(CROSSING_SUBSTRAIT_ROOT ${CROSSING_ROOT}/src/render/substrait)

function(crossing_substrait_render_sources out_var)
  set(${out_var}
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_types.cpp
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_render.cpp
      PARENT_SCOPE)
endfunction()

function(crossing_substrait_decode_sources out_var)
  set(${out_var}
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_types.cpp
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_decode.cpp
      PARENT_SCOPE)
endfunction()

function(crossing_substrait_sources out_var)
  set(${out_var}
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_types.cpp
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_render.cpp
      ${CROSSING_SUBSTRAIT_ROOT}/substrait_decode.cpp
      PARENT_SCOPE)
endfunction()

function(crossing_substrait_includes out_var)
  set(${out_var}
      ${CROSSING_SUBSTRAIT_ROOT}/include ${CROSSING_SUBSTRAIT_ROOT} ${CROSSING_ROOT}/include
      PARENT_SCOPE)
endfunction()
