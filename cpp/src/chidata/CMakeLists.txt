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


add_custom_target(lqf)

add_subdirectory(sboost)

set(LQF_SRCS
        bitmap.cc
        lqf/data_model.cc
        lqf/filter.cc
        lqf/agg.cc
        lqf/join.cc
        lqf/print.cc
        lqf/sort.cc
        )

include_directories(chidata)

add_arrow_lib(lqf
        BUILD_STATIC
        SOURCES
        ${LQF_SRCS}
        OUTPUTS
        LQF_LIBRARIES
        STATIC_LINK_LIBS
        parquet_static
        sboost_static
        )
target_compile_options(lqf_objlib PUBLIC "-mavx2")

set(LQF_TEST_SRC
        bitmap_test.cc
        stream_test.cc
        heap_test.cc
        lqf/data_model_test.cc
        lqf/filter_test.cc
        lqf/agg_test.cc
        lqf/join_test.cc
        lqf/print_test.cc
        lqf/sort_test.cc
        )

add_test_case(lqf-test
        PREFIX
        chidata
        SOURCES
        ${LQF_TEST_SRC}
        STATIC_LINK_LIBS
        lqf_static
        GTest::GTest
        GTest::Main
        )

# Copy all test files
file(GLOB TEST_RES_FILES "resources/*")
string(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_LOWER)
foreach (TEST_RES_FILE ${TEST_RES_FILES})
    get_filename_component(TEST_RES_NAME ${TEST_RES_FILE} NAME)
    configure_file(${TEST_RES_FILE} ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE_LOWER}/${TEST_RES_NAME} COPYONLY)
endforeach ()

arrow_install_all_headers(chidata)

add_executable(scan_column tool/scan_column.cc)
target_link_libraries(scan_column lqf_static parquet_static)