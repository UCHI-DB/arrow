//
// Created by harper on 2/25/20.
//

#include <gtest/gtest.h>
#include "join.h"

using namespace chidata::lqf;

TEST(HashJoinTest, Join) {

    auto left = ParquetTable::Open("lineitem");
    left->updateColumns(7);
    auto right = MemTable::Make(2);
    auto block = right->allocate(100);

    auto rows = block->rows();
    (*rows)[0][0] = 35;
    (*rows)[1][0] = 99;
    (*rows)[2][0] = 121;
    (*rows)[3][0] = 132;
    (*rows)[4][0] = 226;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;

    RowBuilder *rb = new RowBuilder({1, 2}, {1});

    HashJoin join(0, 0, rb);

    auto joined = join.join(*left, *right);

    auto blocks = joined->blocks()->collect();

    auto rblock = (*blocks)[0];

    EXPECT_EQ(21, rblock->size());

    auto rrows = rblock->rows();
    EXPECT_EQ(1, (*rrows)[0][0].asInt());
    EXPECT_EQ(4, (*rrows)[0][1].asInt());
    EXPECT_EQ(0, (*rrows)[0][2].asInt());
    EXPECT_EQ(162, (*rrows)[1][0].asInt());
    EXPECT_EQ(1, (*rrows)[1][1].asInt());
    EXPECT_EQ(0, (*rrows)[1][2].asInt());
    EXPECT_EQ(121, (*rrows)[2][0].asInt());
    EXPECT_EQ(4, (*rrows)[2][1].asInt());
    EXPECT_EQ(0, (*rrows)[2][2].asInt());
}

TEST(HashFilterJoinTest, Join) {

}

TEST(HashExistJoinTest, Join) {

}