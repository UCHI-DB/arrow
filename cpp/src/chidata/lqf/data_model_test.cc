//
// Created by harper on 2/14/20.
//

#include <gtest/gtest.h>
#include "data_model.cc"

using namespace chidata::lqf;


TEST(MemBlockTest, Column) {
    MemBlock mb(100, 5);

    auto col = mb.col(0);

    (*col)[4] = 3.27f;

    ASSERT_DOUBLE_EQ(3.27f, (*col)[4].asDouble());
}

TEST(MemBlockTest, Row) {
    MemBlock mb(100, 5);

    auto row = mb.rows();

    (*row)[4][0] = 4;
    ASSERT_EQ(4, (*row)[4][0].asInt());
}

TEST(MemBlockTest, Mask) {
    MemBlock mb(100, 5);
    auto roww = mb.rows();
    for (int i = 0; i < 100; ++i) {
        (*roww)[i][0] = i;
    }
    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    auto masked = mb.mask(bitmap);

    EXPECT_EQ(3, masked->size());
    auto rows = masked->rows();

    EXPECT_EQ(4, (*rows)[0][0].asInt());
    EXPECT_EQ(20, (*rows)[1][0].asInt());
    EXPECT_EQ(95, (*rows)[2][0].asInt());
}

class ParquetBlockTest : public ::testing::Test {
protected:
    shared_ptr<RowGroupReader> rowGroup_;
public:
    virtual void SetUp() override {
        auto fileReader = ParquetFileReader::OpenFile("lineitem");
        rowGroup_ = fileReader->RowGroup(0);
    }
};

TEST_F(ParquetBlockTest, Column) {
    auto block = unique_ptr<ParquetBlock>(new ParquetBlock(rowGroup_, 0, 3));
    auto col = block->col(1);
    EXPECT_EQ(156, (*col)[0].asInt());
    EXPECT_EQ(68, (*col)[1].asInt());
    EXPECT_EQ(64, (*col)[2].asInt());
}

TEST_F(ParquetBlockTest, Row) {
    auto block = unique_ptr<ParquetBlock>(new ParquetBlock(rowGroup_, 0, 3));
    auto rows = block->rows();

    EXPECT_EQ(1, (*rows)[2][0]);
    EXPECT_EQ(25, (*rows)[4][1]);
}

TEST_F(ParquetBlockTest, Mask) {
    auto block = unique_ptr<ParquetBlock>(new ParquetBlock(rowGroup_, 0, 3));

    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    auto masked = block->mask(bitmap);

    EXPECT_EQ(3, masked->size());
    auto rows = masked->rows();
    EXPECT_EQ(1, (*rows)[0][0].asInt());
    EXPECT_EQ(7, (*rows)[1][0].asInt());
    EXPECT_EQ(97, (*rows)[2][0].asInt());
}

TEST(MemTableTest, Create) {
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

