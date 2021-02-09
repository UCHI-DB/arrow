//
// Created by harper on 2/8/21.
//

#include <gtest/gtest.h>
#include "encoding.h"

using namespace lqf::encoding;

TEST(Bitpack, Encoding) {
    auto encoder = GetEncoder<parquet::Int32Type>(EncodingType::BITPACK);
    for (int i = 0; i < 10000; ++i) {
        encoder->Add(i % 500);
    }
    encoder->Dump();
}

TEST(Bitpack, EncDec) {
    auto encoder = GetEncoder<parquet::Int32Type>(EncodingType::BITPACK);
    for (int i = 0; i < 10000; ++i) {
        encoder->Add(i);
    }
    auto data = encoder->Dump();

    auto decoder = GetDecoder<parquet::Int32Type>(EncodingType::BITPACK);
    decoder->SetData(data);

    int *buffer = (int*)aligned_alloc(256, sizeof(int32_t)*8);
    int loaded = 0;
    for (int i = 0; i < 10000; i += 8) {
        decoder->Decode(buffer, 8);
        if (i + 8 < 10000) {
            for (int j = 0; j < 8; ++j) {
                ASSERT_EQ(buffer[j], i + j);
            }
        } else {
            int limit = 10000 - i;
            for (int j = 0; j < limit; ++j) {
                ASSERT_EQ(buffer[j], i + j);
            }
        }
    }
    free(buffer);
}