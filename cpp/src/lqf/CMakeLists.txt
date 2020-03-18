# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(LQF_SIMD_FLAGS -msse4.1 -mavx -mavx2 -mavx512f -mavx512bw -mavx512dq -mavx512vl -mbmi2)

set(LQF_SRCS
        executor.cc
        stream.cc
        agg.cc
        bitmap.cc
        data_model.cc
        filter.cc
        join.cc
        print.cc
        sort.cc
        mat.cc
        )

add_arrow_lib(lqf
        BUILD_STATIC
        BUILD_SHARED
        SOURCES
        ${LQF_SRCS}
        OUTPUTS
        LQF_LIBRARIES
        STATIC_LINK_LIBS
        parquet_static
        sboost_static
        SHARED_LINK_LIBS
        parquet_static
        sboost_static
        )

target_compile_options(lqf_objlib PUBLIC ${LQF_SIMD_FLAGS} )

set(LQF_TEST_SRC
        bitmap_test.cc
        executor_test.cc
        stream_test.cc
        heap_test.cc
        data_model_test.cc
        filter_test.cc
        agg_test.cc
        join_test.cc
        print_test.cc
        sort_test.cc
        mat_test.cc
        )

add_test_case(all-test
        PREFIX
        lqf
        SOURCES
        ${LQF_TEST_SRC}
        STATIC_LINK_LIBS
        lqf_static
        GTest::GTest
        GTest::Main
        EXTRA_FLAGS
        ${LQF_SIMD_FLAGS}
        )

arrow_install_all_headers(lqf)


add_executable(scan_column scan_column.cc)
target_link_libraries(scan_column lqf_static)

add_subdirectory(tpch)