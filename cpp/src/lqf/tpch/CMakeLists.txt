set(TPCH_SRC
        tpchquery.cc
        query1.cc
        query2.cc
        query3.cc
        query4.cc
        query5.cc
        query6.cc
        query7.cc
        query8.cc
        query9.cc
        query10.cc
        query11.cc
        query12.cc
        query13.cc
        query14.cc
        query15.cc
        query16.cc
        query17.cc
        query18.cc
        query19.cc
        query20.cc
        query21.cc
        query22.cc
        )

add_library(tpch_static ${TPCH_SRC})
target_link_libraries(tpch_static PUBLIC lqf_static)
target_compile_options(tpch_static PUBLIC ${LQF_SIMD_FLAGS})

add_lqf_benchmark(tpchquery_benchmark)

add_executable(lqf-tpch-query-dev tpchquery_dev.cc)
target_link_libraries(lqf-tpch-query-dev PUBLIC tpch_static)
target_compile_options(lqf-tpch-query-dev PUBLIC ${LQF_SIMD_FLAGS})
target_link_options(lqf-tpch-query-dev
        PUBLIC
        $<$<COMPILE_LANGUAGE:CXX>:$<$<CONFIG:Debug>:-fsanitize=address>>)
target_link_options(lqf-tpch-query-dev PUBLIC $<$<COMPILE_LANGUAGE:CXX>:$<$<CONFIG:Debug>:-fsanitize=address>>)
