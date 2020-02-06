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
        bitmap.cc)

add_arrow_lib(chidata
        BUILD_STATIC
        SOURCES
        ${CHIDATA_SRCS}
        OUTPUTS
        CHIDATA_LIBRARIES
        )

target_compile_options(chidata_objlib PUBLIC "-mavx2")

set(CHIDATA_TEST_SRC
        bitmap_test.cc)

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

arrow_install_all_headers(chidata)
