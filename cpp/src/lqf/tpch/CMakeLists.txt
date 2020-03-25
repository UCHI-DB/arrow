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

