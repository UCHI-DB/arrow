//
// Created by harper on 2/2/21.
//

#include <gtest/gtest.h>
#include "data_model_enc.cc"

using namespace lqf;
using namespace parquet;

TEST(EncMemvBlockTest, WriteDictCol) {
    auto block = shared_ptr<EncMemvBlock>(
            new EncMemvBlock({Type::type::INT32, Type::type::INT32},
                             {encoding::EncodingType::DICTIONARY, encoding::EncodingType::PLAIN}));

    auto col = block->col(0);

    for (int i = 0; i < 100; ++i) {
        col->next() = i;
    }
    col->close();

    EXPECT_EQ(block->size(), 100);

    auto block2 = shared_ptr<EncMemvBlock>(
            new EncMemvBlock({Type::type::INT32, Type::type::INT32},
                             {encoding::EncodingType::DICTIONARY, encoding::EncodingType::PLAIN}));

    auto col2 = block2->col(1);

    for (int i = 0; i < 100; ++i) {
        col2->next() = i;
    }
    col2->close();

    EXPECT_EQ(block2->size(), 100);
}

TEST(EncMemvBlockTest, WriteAndReadCol) {
    auto block = shared_ptr<EncMemvBlock>(
            new EncMemvBlock({Type::type::INT32, Type::type::INT32},
                             {encoding::EncodingType::DICTIONARY, encoding::EncodingType::DICTIONARY}));

    auto writecol = block->col(0);

    for (int i = 0; i < 50000; ++i) {
        writecol->next() = i;
    }
    writecol->close();

    EXPECT_EQ(block->size(), 50000);

    auto readcol = block->col(0);
    for (auto i = 0; i < 50000; ++i) {
        EXPECT_EQ(i, readcol->next().asInt());
    }
    readcol->close();
}