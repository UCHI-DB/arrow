
set(SBOOST_SRC
        bitmap_writer.cc
        byteutils.cc
        sboost.cc
        unpacker.cc
        encoding/deltabp.cc
        encoding/encoding_utils.cc
        encoding/rlehybrid.cc
        )

add_library(sboost_static STATIC ${SBOOST_SRC})
set_target_properties(sboost_static
        PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}" OUTPUT_NAME sboost_static)
target_compile_options(sboost_static PUBLIC -msse4.1 -mavx2 -mavx512f -mavx512bw -mavx512dq -mavx512vl -mbmi2 -pthread -fvisibility=hidden)

set(SBOOST_TEST_SRC
        bitmap_writer_test.cc
        byteutils_test.cc
        sboost_test.cc
        unpacker_test.cc
        encoding/rlehybrid_test.cc
        encoding/deltabp_test.cc)

add_test_case(sboost-test
        PREFIX
        chidata
        SOURCES
        ${SBOOST_TEST_SRC}
        STATIC_LINK_LIBS
        sboost_static
        GTest::GTest
        GTest::Main
        )