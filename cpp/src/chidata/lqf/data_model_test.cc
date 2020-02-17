//
// Created by harper on 2/14/20.
//

#include <gtest/gtest.h>
#include "data_model.cc"

using namespace chidata::lqf;


TEST(MemBlock, Column) {
    MemBlock mb(100, 5);

    auto col = mb.col(0);

    (*col)[4] = 3.27f;

    ASSERT_DOUBLE_EQ(3.27f, (*col)[4].asDouble());
}

TEST(MemBlock, Row) {
    MemBlock mb(100, 5);

    auto row = mb.rows();

    (*row)[4][0] = 4;
    ASSERT_EQ(4, (*row)[4][0].asInt());
}

TEST(ParquetBlock, Column) {

}

TEST(ParquetBlock, Row) {

}

TEST(MemTable, Create) {
    auto mt = MemTable::Make(5);

    mt->allocate(100);
    mt->allocate(200);
    mt->allocate(300);

    auto stream = mt->blocks();

    auto list = *(stream->collect());

    ASSERT_EQ(3, list.size());
    ASSERT_EQ(100, list[0]->size());
    ASSERT_EQ(200, list[1]->size());
    ASSERT_EQ(300, list[2]->size());


}

