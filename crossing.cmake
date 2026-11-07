set(CROSSING_ROOT ${CMAKE_CURRENT_LIST_DIR})

set(CROSSING_ENGINE_SOURCE_NAMES
    crossing.cpp
    carrier.cpp
    fragment.cpp
    seam.cpp
    floor.cpp
    source_evaluation.cpp
    rules.cpp
    labelling.cpp
    fold.cpp
    filter_halves.cpp
    fill.cpp
    pass.cpp
    scan_columns.cpp
    table_indices.cpp
    execute.cpp)

set(CROSSING_LIBRARY_SOURCE_NAMES
    ${CROSSING_ENGINE_SOURCE_NAMES}
    crossing_attach.cpp
    crossing_catalog.cpp
    crossing_table_entry.cpp
    crossing_read.cpp
    parking.cpp
    seam_inbox.cpp
    stream_sink.cpp
    far_plan.cpp
    crossing_write.cpp
    crossing_fill.cpp
    crossing_pass.cpp
    crossing_transactions.cpp
    crossing_register.cpp
    crossing_far.cpp
    row_policy.cpp
    crossing_remote.cpp
    crossing_in_process.cpp
    shaped_write.cpp
    render/substrait/substrait_types.cpp
    render/substrait/substrait_render.cpp
    render/substrait/substrait_decode.cpp)

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
  crossing_substrait_includes(result)
  set(${out_var}
      ${result}
      PARENT_SCOPE)
endfunction()

function(crossing_add_to_target target)
  crossing_sources(sources)
  target_sources(${target} PRIVATE ${sources})
  crossing_substrait_includes(substrait_includes)
  target_include_directories(${target} PRIVATE ${CROSSING_ROOT}/include ${substrait_includes})
endfunction()

set(CROSSING_SUBSTRAIT_ROOT ${CROSSING_ROOT}/src/render/substrait)

function(crossing_substrait_includes out_var)
  set(${out_var}
      ${CROSSING_SUBSTRAIT_ROOT}/include ${CROSSING_SUBSTRAIT_ROOT} ${CROSSING_ROOT}/include
      PARENT_SCOPE)
endfunction()
