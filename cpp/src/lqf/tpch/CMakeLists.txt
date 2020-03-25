function(ADD_LQF_BENCHMARK REL_TEST_NAME)
    set(options)
    set(one_value_args PREFIX)
    set(multi_value_args)
    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if (ARG_PREFIX)
        set(PREFIX ${ARG_PREFIX})
    else ()
        set(PREFIX "lqf")
    endif ()
    add_benchmark(${REL_TEST_NAME}
            PREFIX
            ${PREFIX}
            LABELS
            "lqf-benchmarks"
            STATIC_LINK_LIBS
            ${LQF_BENCHMARK_LINK_OPTION}
            EXTRA_FLAGS
            ${LQF_SIMD_FLAGS}
            ${ARG_UNPARSED_ARGUMENTS})
endfunction()


#add_executable(q1_benchmark q1_benchmark.cc)
#target_link_libraries(q1_benchmark PUBLIC lqf_static)
#target_compile_options(q1_benchmark PUBLIC ${LQF_SIMD_FLAGS})
#
#add_executable(q2_benchmark q2_benchmark.cc)
#target_link_libraries(q2_benchmark PUBLIC lqf_static)
#target_compile_options(q2_benchmark PUBLIC ${LQF_SIMD_FLAGS})

set(TPCH_SRC
        tpchquery.cc
        query1.cc
        query2.cc
        query3.cc)

add_library(tpch_static ${TPCH_SRC})
target_link_libraries(tpch_static PUBLIC lqf_static)
target_compile_options(tpch_static PUBLIC ${LQF_SIMD_FLAGS})

add_custom_target(lqf-benchmarks)
set(LQF_BENCHMARK_LINK_OPTION benchmark::benchmark benchmark::benchmark_main Threads::Threads tpch_static)
add_lqf_benchmark(tpchquery_benchmark)

