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


add_custom_target(chidata)

set(CHIDATA_SRCS
        bitmap.cc
        lqf/data_model.cc
        lqf/operator/filter.cc
        lqf/operator/agg.cc
        )

include_directories(chidata)

add_arrow_lib(chidata
        BUILD_STATIC
        SOURCES
        ${CHIDATA_SRCS}
        OUTPUTS
        CHIDATA_LIBRARIES
        STATIC_LINK_LIBS
        parquet_static
        )
target_compile_options(chidata_objlib PUBLIC "-mavx2")

set(CHIDATA_TEST_SRC
        bitmap_test.cc
        stream_test.cc
        lqf/data_model_test.cc
        )

add_test_case(all-test
        PREFIX
        chidata
        SOURCES
        ${CHIDATA_TEST_SRC}
        STATIC_LINK_LIBS
        chidata_static
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
target_link_libraries(scan_column chidata_static parquet_static)