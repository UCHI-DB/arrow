//
// Created by harper on 2/2/21.
//

#include <gtest/gtest.h>
#include "data_model_enc.cc"

using namespace lqf;

TEST(EncMemvBlockTest, WriteCol) {
    auto block = shared_ptr<EncMemvBlock>(
            new EncMemvBlock({encoding::Type::DICTIONARY, encoding::Type::DICTIONARY}));

    auto col = block->col(0);

    for (int i = 0; i < 100; ++i) {
        col->next() = i;
    }
    col->close();

    EXPECT_EQ(block->size(), 100);
}

TEST(EncMemvBlockTest, WriteAndReadCol) {
    auto block = shared_ptr<EncMemvBlock>(
            new EncMemvBlock({encoding::Type::DICTIONARY, encoding::Type::DICTIONARY}));

    auto writecol = block->col(0);

    for (int i = 0; i < 100; ++i) {
        writecol->next() = i;
    }
    writecol->close();

    EXPECT_EQ(block->size(), 100);

    auto readcol = block->col(0);
    for (auto i = 0; i < 100; ++i) {
        EXPECT_EQ(i, readcol->next().asInt());
    }
    readcol->close();
}