//
// Created by harper on 3/17/20.
//

#include <gtest/gtest.h>
#include "mat.h"

using namespace lqf;

TEST(MemMatTest, Mat) {

    MemMat mat(3, MLB MI(0, 0)
        MI(1, 1)
        MD(5, 2) MLE);

    auto parquetTable = ParquetTable::Open("lineitem", 0x27);
    auto matted = mat.mat(*parquetTable);
    auto content = matted->blocks()->collect();

    EXPECT_EQ(1, content->size());

    EXPECT_EQ(6005, (*content)[0]->size());
    auto rows = (*content)[0]->rows();

    EXPECT_EQ(1, (*rows)[0][0].asInt());
    EXPECT_EQ(156, (*rows)[0][1].asInt());
    EXPECT_NEAR(17954.5499, (*rows)[0][2].asDouble(),0.001);
    EXPECT_EQ(1, (*rows)[2][0].asInt());
    EXPECT_EQ(64, (*rows)[2][1].asInt());
    EXPECT_NEAR(7712.4799, (*rows)[2][2].asDouble(),0.001);
    EXPECT_EQ(3, (*rows)[10][0].asInt());
    EXPECT_EQ(30, (*rows)[10][1].asInt());
    EXPECT_NEAR(1860.0599, (*rows)[10][2].asDouble(),0.001);
}

TEST(FilterMatTest, Mat) {
    throw "not implemented";
}