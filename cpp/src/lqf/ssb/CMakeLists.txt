function(ADD_SSB_EXEC NAME SOURCE)
    add_executable(${NAME} ${SOURCE})
    target_link_libraries(${NAME} PUBLIC ssb_static)
    target_compile_options(${NAME} PUBLIC ${LQF_SIMD_FLAGS})
endfunction()


set(SSB_SRC
        ssbquery.cc
        query1_1.cc
        query1_2.cc
        query1_3.cc
        query2_1.cc
        query2_2.cc
        query2_3.cc
        query3_1.cc
        query3_2.cc
        query3_3.cc
        query4_1.cc
        query4_2.cc
        query4_3.cc
        )

add_library(ssb_static ${SSB_SRC})
target_link_libraries(ssb_static PUBLIC lqf_static)
target_compile_options(ssb_static PUBLIC ${LQF_SIMD_FLAGS})

add_ssb_exec(lqf-ssb-query-dev ssbquery_dev.cc)
