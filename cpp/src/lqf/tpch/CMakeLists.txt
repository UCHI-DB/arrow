
add_executable(q1_benchmark q1_benchmark.cc)
target_link_libraries(q1_benchmark PUBLIC lqf_static)
target_compile_options(q1_benchmark PUBLIC ${LQF_SIMD_FLAGS})

add_executable(q2_benchmark q2_benchmark.cc)
target_link_libraries(q2_benchmark PUBLIC lqf_static)
target_compile_options(q2_benchmark PUBLIC ${LQF_SIMD_FLAGS})