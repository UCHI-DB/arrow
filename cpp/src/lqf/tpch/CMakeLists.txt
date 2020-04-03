set(TPCH_SRC
        tpchquery.cc
        query1.cc
        query3.cc
        query4.cc
        )

add_library(tpch_static ${TPCH_SRC})
target_link_libraries(tpch_static PUBLIC lqf_static)
target_compile_options(tpch_static PUBLIC ${LQF_SIMD_FLAGS})

add_custom_target(lqf-benchmarks)
set(LQF_BENCHMARK_LINK_LIBS ${LQF_BENCHMARK_LINK_LIBS} tpch_static)
add_lqf_benchmark(tpchquery_benchmark)

add_executable(lqf-tpch-query2 query2.cc)
target_link_libraries(lqf-tpch-query2 PUBLIC tpch_static)
target_compile_options(lqf-tpch-query2 PUBLIC ${LQF_SIMD_FLAGS})
