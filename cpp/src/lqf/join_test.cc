//
// Created by harper on 2/25/20.
//

#include <gtest/gtest.h>
#include <tuple>
#include "join.h"

using namespace lqf;

TEST(RowBuilderTest, Create) {
    RowBuilder rb({JL(0), JL(1), JR(0), JR(2)});
    rb.init();
    EXPECT_EQ(vector<uint32_t>({1, 1, 1, 1}), rb.outputColSize());
//    EXPECT_EQ(vector<uint32_t>({0, 1, 2, 3, 4}), rb.outputColOffset());
    EXPECT_EQ(false, rb.useVertical());

    MemDataRow left(4);
    MemDataRow right(3);

    left[0] = 424;
    left[1] = 3243;
    left[2] = 87452;
    left[3] = 21232323;
    right[0] = 33244;
    right[1] = 34359543;
    right[2] = 33901;

    auto snapshoter = rb.snapshoter();
    auto lsn = (*snapshoter)(left);
    auto rsn = (*snapshoter)(right);
    EXPECT_EQ(424, (*lsn)[0].asInt());
    EXPECT_EQ(87452, (*lsn)[1].asInt());
    EXPECT_EQ(33244, (*rsn)[0].asInt());
    EXPECT_EQ(33901, (*rsn)[1].asInt());

    MemDataRow output(4);

    rb.build(output, left, right, 0);
    EXPECT_EQ(424, output[0].asInt());
    EXPECT_EQ(3243, output[1].asInt());
    EXPECT_EQ(33244, output[2].asInt());
    EXPECT_EQ(34359543, output[3].asInt());
}

TEST(RowBuilderTest, CreateWithString) {
    RowBuilder rb({JL(0), JL(1), JR(0), JR(2), JLS(3), JRS(3)}, true);
    rb.init();
    EXPECT_EQ(vector<uint32_t>({1, 1, 1, 1, 1, 2, 2}), rb.outputColSize());
//    EXPECT_EQ(vector<uint32_t>({0, 1, 2, 3, 4, 5, 7, 9}), rb.outputColOffset());
    EXPECT_EQ(false, rb.useVertical());

    const vector <uint32_t> offset{0, 1, 2, 3, 5};
    const vector <uint32_t> offset2{0, 1, 2, 4};

    MemDataRow left(offset);
    MemDataRow right(offset2);

    ByteArray bytedata("ref");
    ByteArray bytedata2("ddb=");

    left[0] = 424;
    left[1] = 3243;
    left[2] = 87452;
    left[3] = bytedata;
    right[0] = 33244;
    right[1] = 34359543;
    right[2] = bytedata2;

    const vector <uint32_t> resoffset{0, 1, 2, 3, 4, 5, 7, 9};
    MemDataRow output(resoffset);

    rb.build(output, left, right, 123214);

    EXPECT_EQ(123214, output[0].asInt());
    EXPECT_EQ(424, output[1].asInt());
    EXPECT_EQ(3243, output[2].asInt());
    EXPECT_EQ(33244, output[3].asInt());
    EXPECT_EQ(34359543, output[4].asInt());
    EXPECT_EQ(bytedata, output[5].asByteArray());
    EXPECT_EQ(bytedata2, output[6].asByteArray());
}

TEST(RowBuilderTest, CreateRaw) {
    RowBuilder rb({JL(0), JL(1), JR(0), JR(2), JLR(3), JRR(3)}, true);
    rb.init();
    EXPECT_EQ(vector<uint32_t>({1, 1, 1, 1, 1, 1, 1}), rb.outputColSize());
    EXPECT_EQ(false, rb.useVertical());

    const vector <uint32_t> offset{0, 1, 2, 3, 4};
    const vector <uint32_t> offset2{0, 1, 2, 3};

    MemDataRow left(offset);
    MemDataRow right(offset2);

    left[0] = 424;
    left[1] = 3243;
    left[2] = 87452;
    left[3] = 3392;
    right[0] = 33244;
    right[1] = 34359543;
    right[2] = 4242;

    const vector <uint32_t> resoffset{0, 1, 2, 3, 4, 5, 6, 7};
    MemDataRow output(resoffset);

    rb.build(output, left, right, 123214);

    EXPECT_EQ(123214, output[0].asInt());
    EXPECT_EQ(424, output[1].asInt());
    EXPECT_EQ(3243, output[2].asInt());
    EXPECT_EQ(33244, output[3].asInt());
    EXPECT_EQ(34359543, output[4].asInt());
    EXPECT_EQ(3392, output[5].asInt());
    EXPECT_EQ(4242, output[6].asInt());
}

class ColumnBuilderForTest : public ColumnBuilder {
public:
    ColumnBuilderForTest(initializer_list <int32_t> list) : ColumnBuilder(list) {}

    vector <pair<uint8_t, uint8_t>> &leftInst() {
        return left_merge_inst_;
    }

    vector <pair<uint8_t, uint8_t>> &leftMemInst() {
        return leftmem_merge_inst_;
    }

    vector <pair<uint8_t, uint8_t>> &rightInst() {
        return right_merge_inst_;
    }
};

TEST(ColumnBuilderTest, Create) {
    ColumnBuilderForTest cb({JL(0), JR(2), JL(2), JR(0)});
    cb.init();
    EXPECT_EQ(true, cb.useVertical());
    EXPECT_EQ(vector<uint32_t>({1, 1, 1, 1}), cb.outputColSize());

    EXPECT_EQ(vector<uint32_t>({1, 1}), cb.rightColSize());
    vector <pair<uint8_t, uint8_t>> vleft({pair<uint8_t, uint8_t>(0, 0), pair<uint8_t, uint8_t>({2, 2})});
    vector <pair<uint8_t, uint8_t>> vmemleft({pair<uint8_t, uint8_t>(0, 0), pair<uint8_t, uint8_t>({1, 2})});
    vector <pair<uint8_t, uint8_t>> vright({pair<uint8_t, uint8_t>(0, 1), pair<uint8_t, uint8_t>({1, 3})});
    EXPECT_EQ(vleft, cb.leftInst());
    EXPECT_EQ(vmemleft, cb.leftMemInst());
    EXPECT_EQ(vright, cb.rightInst());
}

TEST(ColumnBuilderTest, CreateWithString) {
    ColumnBuilderForTest cb({JL(0), JL(1), JR(2), JR(0), JLS(3), JRS(3)});
    cb.init();
    EXPECT_EQ(true, cb.useVertical());
    EXPECT_EQ(vector<uint32_t>({1, 1, 1, 1, 2, 2}), cb.outputColSize());
    EXPECT_EQ(vector<uint32_t>({0, 1, 2, 3, 4, 6, 8}), cb.outputColOffset());

    EXPECT_EQ(vector<uint32_t>({1, 1, 2}), cb.rightColSize());
    vector <pair<uint8_t, uint8_t>> vleft(
            {pair<uint8_t, uint8_t>(0, 0), pair<uint8_t, uint8_t>({1, 1}), pair<uint8_t, uint8_t>(3, 4)});
    vector <pair<uint8_t, uint8_t>> vmemleft(
            {pair<uint8_t, uint8_t>(0, 0), pair<uint8_t, uint8_t>({1, 1}), pair<uint8_t, uint8_t>(2, 4)});
    vector <pair<uint8_t, uint8_t>> vright({pair<uint8_t, uint8_t>(0, 2), pair<uint8_t, uint8_t>({1, 3}),
                                            pair<uint8_t, uint8_t>(2, 5)});
    EXPECT_EQ(vleft, cb.leftInst());
    EXPECT_EQ(vmemleft, cb.leftMemInst());
    EXPECT_EQ(vright, cb.rightInst());
}

TEST(ColumnBuilderTest, cacheToMem) {
    ColumnBuilderForTest cb({JL(0), JL(1), JR(2), JR(0), JL(2), JL(3)});
    cb.init();

    auto ptable = ParquetTable::Open("testres/lineitem", {0, 1, 2, 3});
    auto blocks = ptable->blocks()->collect();
    auto first_block = (*blocks)[0];

    auto first_block_size = first_block->size();
    auto mask = make_shared<SimpleBitmap>(first_block_size);
    for (uint32_t i = 0; i < first_block_size; ++i) {
        if (i % 5 == 0) {
            mask->put(i);
        }
    }
    auto masked = first_block->mask(mask);
    auto cached = cb.cacheToMem(*masked);

    auto cached_size = first_block_size / 5;
    EXPECT_EQ(cached_size, cached->size());

    auto ptable2 = ParquetTable::Open("testres/lineitem", {0, 1, 2, 3});
    auto blocks2 = ptable2->blocks()->collect();
    auto first_block2 = (*blocks2)[0];

    auto cached_rows = cached->rows();
    auto origin_rows = first_block2->rows();
    for (uint32_t i = 0; i < cached_size; ++i) {
        EXPECT_EQ((*cached_rows)[i][0].asInt(), (*origin_rows)[5 * i][0].asInt()) << i;
        EXPECT_EQ((*cached_rows)[i][1].asInt(), (*origin_rows)[5 * i][1].asInt()) << i;
        EXPECT_EQ((*cached_rows)[i][2].asInt(), (*origin_rows)[5 * i][2].asInt()) << i;
        EXPECT_EQ((*cached_rows)[i][3].asInt(), (*origin_rows)[5 * i][3].asInt()) << i;
    }
}

TEST(ColumnBuilderTest, BuildFromMem) {
    ColumnBuilderForTest cb({JL(0), JR(0), JL(1)});
    cb.init();
    auto leftBlock = make_shared<MemvBlock>(10, lqf::colSize(2));
    auto rightBlock = make_shared<MemvBlock>(10, lqf::colSize(1));

    auto leftWriter = leftBlock->rows();
    auto rightWriter = rightBlock->rows();
    for (int i = 0; i < 10; ++i) {
        (*leftWriter)[i][0] = i;
        (*leftWriter)[i][1] = 2 * i;
        (*rightWriter)[i][0] = i + 3.75;
    }
    auto mergedBlock = make_shared<MemvBlock>(0, lqf::colSize(3));
    cb.buildFromMem(*mergedBlock, *leftBlock, *rightBlock);

    EXPECT_EQ(10, mergedBlock->size());
    auto reader = mergedBlock->rows();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ((*reader)[i][0].asInt(), i) << i;
        EXPECT_EQ((*reader)[i][1].asDouble(), i + 3.75) << i;
        EXPECT_EQ((*reader)[i][2].asInt(), 2 * i) << i;
    }
}

TEST(HashJoinTest, JoinWithoutKey) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(3);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;

    auto block2 = right->allocate(2);
    rows = block2->rows();
    (*rows)[0][0] = 4452; // 2
    (*rows)[1][0] = 5987; // 4
    (*rows)[0][1] = 3;
    (*rows)[1][1] = 4;


    HashJoin join1(0, 0, new RowBuilder({JL(1), JL(2), JR(1), JLS(10)}));

    auto joined = join1.join(*left, *right);
    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(22, rblock->size());

    auto rrows = rblock->rows();
    int i = 0;

    {
        EXPECT_EQ(1, (*rrows)[i][0].asInt());
        EXPECT_EQ(4, (*rrows)[i][1].asInt());
        EXPECT_EQ(0, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-02-21"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(162, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(0, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-01-22"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(121, (*rrows)[i][0].asInt());
        EXPECT_EQ(4, (*rrows)[i][1].asInt());
        EXPECT_EQ(0, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-01-19"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(86, (*rrows)[i][0].asInt());
        EXPECT_EQ(7, (*rrows)[i][1].asInt());
        EXPECT_EQ(0, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1995-11-26"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(120, (*rrows)[i][0].asInt());
        EXPECT_EQ(7, (*rrows)[i][1].asInt());
        EXPECT_EQ(0, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1995-11-08"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(31, (*rrows)[i][0].asInt());
        EXPECT_EQ(7, (*rrows)[i][1].asInt());
        EXPECT_EQ(0, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-02-01"), (*rrows)[i][3].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(88, (*rrows)[i][0].asInt());
        EXPECT_EQ(9, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1994-05-18"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(124, (*rrows)[i][0].asInt());
        EXPECT_EQ(5, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1994-05-06"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(135, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1994-04-19"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(109, (*rrows)[i][0].asInt());
        EXPECT_EQ(2, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1994-07-04"), (*rrows)[i][3].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(143, (*rrows)[i][0].asInt());
        EXPECT_EQ(10, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1992-04-17"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(148, (*rrows)[i][0].asInt());
        EXPECT_EQ(7, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1992-04-22"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(97, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1992-06-07"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(1, (*rrows)[i][0].asInt());
        EXPECT_EQ(2, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1992-03-30"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(36, (*rrows)[i][0].asInt());
        EXPECT_EQ(2, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1992-02-26"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(196, (*rrows)[i][0].asInt());
        EXPECT_EQ(8, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1992-03-04"), (*rrows)[i][3].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(114, (*rrows)[i][0].asInt());
        EXPECT_EQ(8, (*rrows)[i][1].asInt());
        EXPECT_EQ(3, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1994-10-06"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(1, (*rrows)[i][0].asInt());
        EXPECT_EQ(8, (*rrows)[i][1].asInt());
        EXPECT_EQ(3, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1994-10-08"), (*rrows)[i][3].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(23, (*rrows)[i][0].asInt());
        EXPECT_EQ(2, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-09-13"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(176, (*rrows)[i][0].asInt());
        EXPECT_EQ(5, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-11-28"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(92, (*rrows)[i][0].asInt());
        EXPECT_EQ(3, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-10-30"), (*rrows)[i][3].asByteArray());
        ++i;

        EXPECT_EQ(97, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(ByteArray("1996-10-15"), (*rrows)[i][3].asByteArray());
    }

}

TEST(HashJoinTest, JoinWithKey) {
    HashJoin join(0, 0, new RowBuilder({JL(1), JL(2), JR(1), JLS(10)}, true));

    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(5);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;


    auto joined = join.join(*left, *right);
    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(22, rblock->size());

    auto rrows = rblock->rows();
    int i = 0;

    {
        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-02-21"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(162, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-01-22"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(121, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-01-19"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(86, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1995-11-26"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(120, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1995-11-08"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(31, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-02-01"), (*rrows)[i][4].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(88, (*rrows)[i][1].asInt());
        EXPECT_EQ(9, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1994-05-18"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(124, (*rrows)[i][1].asInt());
        EXPECT_EQ(5, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1994-05-06"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(135, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1994-04-19"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(109, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1994-07-04"), (*rrows)[i][4].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(143, (*rrows)[i][1].asInt());
        EXPECT_EQ(10, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1992-04-17"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(148, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1992-04-22"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(97, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1992-06-07"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1992-03-30"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(36, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1992-02-26"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(196, (*rrows)[i][1].asInt());
        EXPECT_EQ(8, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1992-03-04"), (*rrows)[i][4].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(4452, (*rrows)[i][0].asInt());
        EXPECT_EQ(114, (*rrows)[i][1].asInt());
        EXPECT_EQ(8, (*rrows)[i][2].asInt());
        EXPECT_EQ(3, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1994-10-06"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(4452, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(8, (*rrows)[i][2].asInt());
        EXPECT_EQ(3, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1994-10-08"), (*rrows)[i][4].asByteArray());
        ++i;
    }

    {
        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(23, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-09-13"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(176, (*rrows)[i][1].asInt());
        EXPECT_EQ(5, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-11-28"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(92, (*rrows)[i][1].asInt());
        EXPECT_EQ(3, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-10-30"), (*rrows)[i][4].asByteArray());
        ++i;

        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(97, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(ByteArray("1996-10-15"), (*rrows)[i][4].asByteArray());
    }
}

TEST(HashJoinTest, WithRawLeft) {
    HashJoin join(0, 0, new RowBuilder({JL(1), JL(2), JR(1), JLR(14)}, true, true));

    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 16) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(5);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;


    auto joined = join.join(*left, *right);
    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(22, rblock->size());

    auto rrows = rblock->rows();
    int i = 0;

    {
        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(1, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(162, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(3, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(121, (*rrows)[i][1].asInt());
        EXPECT_EQ(4, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(2, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(86, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(5, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(120, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(2, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(35, (*rrows)[i][0].asInt());
        EXPECT_EQ(31, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(0, (*rrows)[i][3].asInt());
        EXPECT_EQ(3, (*rrows)[i][4].asInt());
        ++i;
    }

    {
        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(88, (*rrows)[i][1].asInt());
        EXPECT_EQ(9, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(3, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(124, (*rrows)[i][1].asInt());
        EXPECT_EQ(5, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(3, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(135, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(3, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(99, (*rrows)[i][0].asInt());
        EXPECT_EQ(109, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(1, (*rrows)[i][3].asInt());
        EXPECT_EQ(0, (*rrows)[i][4].asInt());
        ++i;
    }

    {
        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(143, (*rrows)[i][1].asInt());
        EXPECT_EQ(10, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(0, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(148, (*rrows)[i][1].asInt());
        EXPECT_EQ(7, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(6, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(97, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(2, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(6, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(36, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(4, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(1154, (*rrows)[i][0].asInt());
        EXPECT_EQ(196, (*rrows)[i][1].asInt());
        EXPECT_EQ(8, (*rrows)[i][2].asInt());
        EXPECT_EQ(2, (*rrows)[i][3].asInt());
        EXPECT_EQ(6, (*rrows)[i][4].asInt());
        ++i;
    }

    {
        EXPECT_EQ(4452, (*rrows)[i][0].asInt());
        EXPECT_EQ(114, (*rrows)[i][1].asInt());
        EXPECT_EQ(8, (*rrows)[i][2].asInt());
        EXPECT_EQ(3, (*rrows)[i][3].asInt());
        EXPECT_EQ(6, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(4452, (*rrows)[i][0].asInt());
        EXPECT_EQ(1, (*rrows)[i][1].asInt());
        EXPECT_EQ(8, (*rrows)[i][2].asInt());
        EXPECT_EQ(3, (*rrows)[i][3].asInt());
        EXPECT_EQ(6, (*rrows)[i][4].asInt());
        ++i;
    }

    {
        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(23, (*rrows)[i][1].asInt());
        EXPECT_EQ(2, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(4, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(176, (*rrows)[i][1].asInt());
        EXPECT_EQ(5, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(3, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(92, (*rrows)[i][1].asInt());
        EXPECT_EQ(3, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(0, (*rrows)[i][4].asInt());
        ++i;

        EXPECT_EQ(5987, (*rrows)[i][0].asInt());
        EXPECT_EQ(97, (*rrows)[i][1].asInt());
        EXPECT_EQ(1, (*rrows)[i][2].asInt());
        EXPECT_EQ(4, (*rrows)[i][3].asInt());
        EXPECT_EQ(2, (*rrows)[i][4].asInt());
    }
}

TEST(HashJoinTest, WithRawRight) {
    HashJoin join(0, 0, new RowBuilder({JL(1), JR(1), JRR(2)}, false));

    auto right = ParquetTable::Open("testres/orders");
    right->updateColumns((1 << 3) - 1);

    auto left = MemTable::Make(2);
    auto block = left->allocate(100);

    auto rows = block->rows();
    (*rows)[0][0] = 7; // 6
    (*rows)[1][0] = 32; // 4
    (*rows)[2][0] = 163; // 6
    (*rows)[3][0] = 418; // 2
    (*rows)[4][0] = 998; // 4
    (*rows)[5][0] = 5988; // 4
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;

    auto joined = join.join(*left, *right);
    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(6, rblock->size());

    auto rrows = rblock->rows();
    int i = 0;

    EXPECT_EQ(0, (*rrows)[i][0].asInt());
    EXPECT_EQ(40, (*rrows)[i][1].asInt());
    EXPECT_EQ(1, (*rrows)[i][2].asInt());
    ++i;
    EXPECT_EQ(1, (*rrows)[i][0].asInt());
    EXPECT_EQ(131, (*rrows)[i][1].asInt());
    EXPECT_EQ(1, (*rrows)[i][2].asInt());
    ++i;
    EXPECT_EQ(2, (*rrows)[i][0].asInt());
    EXPECT_EQ(88, (*rrows)[i][1].asInt());
    EXPECT_EQ(1, (*rrows)[i][2].asInt());
    ++i;
    EXPECT_EQ(3, (*rrows)[i][0].asInt());
    EXPECT_EQ(95, (*rrows)[i][1].asInt());
    EXPECT_EQ(2, (*rrows)[i][2].asInt());
    ++i;
    EXPECT_EQ(4, (*rrows)[i][0].asInt());
    EXPECT_EQ(32, (*rrows)[i][1].asInt());
    EXPECT_EQ(0, (*rrows)[i][2].asInt());
    ++i;
    EXPECT_EQ(5, (*rrows)[i][0].asInt());
    EXPECT_EQ(31, (*rrows)[i][1].asInt());
    EXPECT_EQ(0, (*rrows)[i][2].asInt());
}

TEST(HashFilterJoinTest, Join) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(100);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;


    FilterJoin join(0, 0);

    auto joined = join.join(*left, *right);
    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(22, rblock->size());
}

TEST(FilterTransformJoinTest, Join) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(100);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;

    auto snapshot = RowCopyFactory().from(EXTERNAL)->to(RAW)->to_layout(colOffset(3))
            ->process([](DataRow &output, DataRow &input) {
                output[0] = input[4].asInt() * input[5].asDouble();
                output[1] = input[4];
                output[2] = input[5];
            })->buildSnapshot();

    FilterTransformJoin join(0, 0, move(snapshot), nullptr);

    auto joined = join.join(*left, *right);
    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(22, rblock->size());

    auto read_rows = rblock->rows();
    for (int i = 0; i < 22; ++i) {
        DataRow &row = (*read_rows)[i];
        EXPECT_EQ(row[0].asDouble(), row[1].asInt() * row[2].asDouble());
    }
}

TEST(HashExistJoinTest, Join) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(8);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[5][0] = 14145;
    (*rows)[6][0] = 21859;
    (*rows)[7][0] = 40;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;
    (*rows)[6][1] = 6;
    (*rows)[7][1] = 7;


    HashExistJoin join(0, 0, new RowBuilder({JR(0), JR(1)}));

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1}), joined->colSize());

    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(5, rblock->size());

    auto read_rows = rblock->rows();

    int i = 0;
    EXPECT_EQ(35, (*read_rows)[i][0].asInt());
    EXPECT_EQ(0, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(99, (*read_rows)[i][0].asInt());
    EXPECT_EQ(1, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(1154, (*read_rows)[i][0].asInt());
    EXPECT_EQ(2, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(4452, (*read_rows)[i][0].asInt());
    EXPECT_EQ(3, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(5987, (*read_rows)[i][0].asInt());
    EXPECT_EQ(4, (*read_rows)[i][1].asInt());
}

TEST(HashExistJoinTest, JoinWithPredicate) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(8);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[5][0] = 14145;
    (*rows)[6][0] = 21859;
    (*rows)[7][0] = 40;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;
    (*rows)[6][1] = 6;
    (*rows)[7][1] = 7;


    // QUANTITY
    HashExistJoin join(0, 0,
                       new RowBuilder({JR(0), JR(1)}),
                       [](DataRow &left, DataRow &right) {
                           return left[4].asInt() > 40;
                       });

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1}), joined->colSize());

    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(4, rblock->size());

    auto read_rows = rblock->rows();

    int i = 0;
    EXPECT_EQ(99, (*read_rows)[i][0].asInt());
    EXPECT_EQ(1, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(1154, (*read_rows)[i][0].asInt());
    EXPECT_EQ(2, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(4452, (*read_rows)[i][0].asInt());
    EXPECT_EQ(3, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(5987, (*read_rows)[i][0].asInt());
    EXPECT_EQ(4, (*read_rows)[i][1].asInt());
}

TEST(HashNotExistJoinTest, Join) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(8);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[5][0] = 141454224;
    (*rows)[6][0] = 218593224;
    (*rows)[7][0] = 40528102;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;
    (*rows)[6][1] = 6;
    (*rows)[7][1] = 7;


    HashNotExistJoin join(0, 0, new RowBuilder({JR(0), JR(1)}));

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1}), joined->colSize());

    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(3, rblock->size());

    auto read_rows = rblock->rows();

    int i = 0;
    EXPECT_EQ(40528102, (*read_rows)[i][0].asInt());
    EXPECT_EQ(7, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(141454224, (*read_rows)[i][0].asInt());
    EXPECT_EQ(5, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(218593224, (*read_rows)[i][0].asInt());
    EXPECT_EQ(6, (*read_rows)[i][1].asInt());
}

TEST(HashNotExistJoinTest, JoinWithPredicate) {
    auto left = ParquetTable::Open("testres/lineitem");
    left->updateColumns((1 << 14) - 1);
    auto right = MemTable::Make(2);
    auto block = right->allocate(8);

    auto rows = block->rows();
    (*rows)[0][0] = 35; // 6
    (*rows)[1][0] = 99; // 4
    (*rows)[2][0] = 1154; // 6
    (*rows)[3][0] = 4452; // 2
    (*rows)[4][0] = 5987; // 4
    (*rows)[5][0] = 141454224;
    (*rows)[6][0] = 218593224;
    (*rows)[7][0] = 40528102;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;
    (*rows)[6][1] = 6;
    (*rows)[7][1] = 7;


    HashNotExistJoin join(0, 0, new RowBuilder({JR(0), JR(1)}),
                          [](DataRow &left, DataRow &right) {
                              return left[4].asInt() > 40;
                          });

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1}), joined->colSize());

    auto blocks = joined->blocks()->collect();
    auto rblock = (*blocks)[0];
    EXPECT_EQ(4, rblock->size());

    auto read_rows = rblock->rows();

    int i = 0;
    EXPECT_EQ(40528102, (*read_rows)[i][0].asInt());
    EXPECT_EQ(7, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(141454224, (*read_rows)[i][0].asInt());
    EXPECT_EQ(5, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(35, (*read_rows)[i][0].asInt());
    EXPECT_EQ(0, (*read_rows)[i][1].asInt());
    ++i;
    EXPECT_EQ(218593224, (*read_rows)[i][0].asInt());
    EXPECT_EQ(6, (*read_rows)[i][1].asInt());
}

TEST(HashColumnJoinTest, Join) {
    auto left = MemTable::Make(2, true);
    auto lblock1 = left->allocate(100);
    auto lblock2 = left->allocate(150);

    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_real_distribution<> dis(1.0, 2.0);

    auto lrow1 = lblock1->rows();
    for (int i = 0; i < 100; i++) {
        int32_t rand = gen() % 10;
        (*lrow1)[i][0] = rand;
        (*lrow1)[i][1] = dis(gen);
    }
    auto lrow2 = lblock2->rows();
    for (int i = 0; i < 150; i++) {
        int32_t rand = gen() % 10;
        (*lrow2)[i][0] = rand;
        (*lrow2)[i][1] = dis(gen);
    }

    auto right = MemTable::Make(2);
    auto block = right->allocate(10);
    auto rows = block->rows();

    array<int32_t, 10> data{35, 99, 1154, 4452, 5987, 14145, 21859, 40, 1230, 3234};

    (*rows)[0][0] = 35;
    (*rows)[1][0] = 99;
    (*rows)[2][0] = 1154;
    (*rows)[3][0] = 4452;
    (*rows)[4][0] = 5987;
    (*rows)[5][0] = 14145;
    (*rows)[6][0] = 21859;
    (*rows)[7][0] = 40;
    (*rows)[8][0] = 1230;
    (*rows)[9][0] = 3234;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;
    (*rows)[6][1] = 6;
    (*rows)[7][1] = 7;
    (*rows)[8][1] = 8;
    (*rows)[9][1] = 9;

    HashColumnJoin join(0, 1, new ColumnBuilder({JL(0), JL(1), JR(0)}));

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1, 1}), joined->colSize());

    auto results = joined->blocks()->collect();
    EXPECT_EQ(results->size(), 2);

    auto block1 = (*results)[0];
    auto asmvblock = dynamic_pointer_cast<MemvBlock>(block1);
    EXPECT_TRUE(asmvblock.get() != nullptr);

    auto block2 = (*results)[1];
    asmvblock = dynamic_pointer_cast<MemvBlock>(block2);
    EXPECT_TRUE(asmvblock.get() != nullptr);

    EXPECT_EQ(100, block1->size());
    auto res_rows = block1->rows();
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(data[(*res_rows)[i][0].asInt()], (*res_rows)[i][2].asInt());
    }

    EXPECT_EQ(150, block2->size());
    res_rows = block2->rows();
    for (int i = 0; i < 150; ++i) {
        EXPECT_EQ(data[(*res_rows)[i][0].asInt()], (*res_rows)[i][2].asInt());
    }
}

TEST(HashColumnJoinTest, JoinWithFilter) {
    auto left = MemTable::Make(2, true);
    auto lblock1 = left->allocate(100);
    auto lblock2 = left->allocate(150);


    auto lrow1 = lblock1->rows();
    for (int i = 0; i < 100; i++) {
        (*lrow1)[i][0] = i % 10;
        (*lrow1)[i][1] = i * 0.1;
    }
    auto lrow2 = lblock2->rows();
    for (int i = 0; i < 150; i++) {
        (*lrow2)[i][0] = i % 10;
        (*lrow2)[i][1] = i * 0.2;
    }

    auto right = MemTable::Make(2);
    auto block = right->allocate(10);
    auto rows = block->rows();

    array<int32_t, 10> data{35, 99, 1154, 4452, 5987, 14145};

    for (int i = 0; i < 6; ++i) {
        (*rows)[i][0] = data[i];
        (*rows)[i][1] = i;
    }

    HashColumnJoin join(0, 1, new ColumnBuilder({JL(0), JL(1), JR(0)}), true);

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1, 1}), joined->colSize());

    auto results = joined->blocks()->collect();
    EXPECT_EQ(results->size(), 2);

    auto block1 = (*results)[0];
    auto asmvblock = dynamic_pointer_cast<MaskedBlock>(block1);
    EXPECT_TRUE(asmvblock.get() != nullptr);

    auto block2 = (*results)[1];
    asmvblock = dynamic_pointer_cast<MaskedBlock>(block2);
    EXPECT_TRUE(asmvblock.get() != nullptr);

    EXPECT_EQ(60, block1->size());
    auto res_rows = block1->rows();
    for (int i = 0; i < 60; ++i) {
        auto key = (*res_rows)[i][0].asInt();
        auto v1 = (*res_rows)[i][1].asDouble();
        auto v2 = (*res_rows)[i][2].asInt();
        EXPECT_EQ(v1, key * 0.1);
        EXPECT_EQ(v2, data[key]);
    }

    EXPECT_EQ(90, block2->size());
    res_rows = block2->rows();
    for (int i = 0; i < 90; ++i) {
        auto key = (*res_rows)[i][0].asInt();
        auto v1 = (*res_rows)[i][1].asDouble();
        auto v2 = (*res_rows)[i][2].asInt();
        EXPECT_EQ(v1, key * 0.2);
        EXPECT_EQ(v2, data[key]);
    }
}

TEST(ParquetHashColumnJoinTest, Join) {
    auto left = ParquetTable::Open("testres/lineitem", {0, 1, 2, 3});

    auto right = MemTable::Make(2);
    auto block = right->allocate(10);
    auto rows = block->rows();

    array<int32_t, 10> data{35, 99, 1154, 4452, 5987, 14145, 21859, 40, 1230, 3234};

    (*rows)[0][0] = 35;
    (*rows)[1][0] = 99;
    (*rows)[2][0] = 1154;
    (*rows)[3][0] = 4452;
    (*rows)[4][0] = 5987;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;

    ParquetHashColumnJoin join(3, 1, new ColumnBuilder({JL(0), JL(1), JL(2), JR(0)}));

    auto joined = join.join(*left, *right);

    int counter = 0;
    vector <tuple<int, int, int, int>> storage;
    auto temp = ParquetTable::Open("testres/lineitem", {0, 1, 2, 3});
    temp->blocks()->sequential()->foreach([&counter, &storage](const shared_ptr <Block> &block) {
        auto rows = block->rows();
        auto size = block->size();
        for (uint32_t i = 0; i < size; ++i) {
            DataRow &row = rows->next();
            auto key = row[3].asInt();
            if (key >= 0 && key <= 4) {
                counter++;
                storage.emplace_back(make_tuple(row[0].asInt(), row[1].asInt(), row[2].asInt(), row[3].asInt()));
            }
        }
    });

    uint32_t table_size = 0;

    joined->blocks()->sequential()->foreach([&storage, &data, &table_size](const shared_ptr <Block> &block) {
        auto rows = block->rows();
        auto size = block->size();
        table_size += size;
        int id1,id2,id3,id4;
        for (uint32_t i = 0; i < size; ++i) {
            DataRow &row = rows->next();
            std::tie(id1,id2,id3,id4) = storage[i];
            EXPECT_EQ(row[0].asInt(),id1);
            EXPECT_EQ(row[1].asInt(),id2);
            EXPECT_EQ(row[2].asInt(),id3);
            EXPECT_EQ(row[3].asInt(),data[id4]);
        }
    });
    EXPECT_EQ(counter, table_size);
}

TEST(HashMultiJoinTest, Join) {
    auto left = MemTable::Make(2, true);
    auto lblock1 = left->allocate(100);
    auto lblock2 = left->allocate(150);

    auto lrow1 = lblock1->rows();
    for (int i = 0; i < 100; i++) {
        (*lrow1)[i][0] = i;
        (*lrow1)[i][1] = i * 2;
    }
    auto lrow2 = lblock2->rows();
    for (int i = 0; i < 150; i++) {
        (*lrow2)[i][0] = i + 100;
        (*lrow2)[i][1] = (i + 100) * 2;
    }

    auto right = MemTable::Make(2);
    auto block = right->allocate(10);
    auto rows = block->rows();


    (*rows)[0][0] = 20;
    (*rows)[1][0] = 20;
    (*rows)[2][0] = 30;
    (*rows)[3][0] = 30;
    (*rows)[4][0] = 30;
    (*rows)[5][0] = 120;
    (*rows)[6][0] = 120;
    (*rows)[7][0] = 140;
    (*rows)[8][0] = 140;
    (*rows)[9][0] = 140;
    (*rows)[0][1] = 0;
    (*rows)[1][1] = 1;
    (*rows)[2][1] = 2;
    (*rows)[3][1] = 3;
    (*rows)[4][1] = 4;
    (*rows)[5][1] = 5;
    (*rows)[6][1] = 6;
    (*rows)[7][1] = 7;
    (*rows)[8][1] = 8;
    (*rows)[9][1] = 9;

    HashMultiJoin join(0, 0, new RowBuilder({JL(1), JR(1)}, true));

    auto joined = join.join(*left, *right);
    EXPECT_EQ(vector<uint32_t>({1, 1, 1}), joined->colSize());

    auto results = joined->blocks()->collect();
    EXPECT_EQ(results->size(), 2);

    auto block1 = (*results)[0];
    auto asmblock = dynamic_pointer_cast<MemFlexBlock>(block1);
    EXPECT_TRUE(asmblock.get() != nullptr);

    auto block2 = (*results)[1];
    asmblock = dynamic_pointer_cast<MemFlexBlock>(block2);
    EXPECT_TRUE(asmblock.get() != nullptr);

    EXPECT_EQ(5, block1->size());
    auto res_rows = block1->rows();
    DataRow &row = res_rows->next();
    EXPECT_EQ(20, row[0].asInt());
    EXPECT_EQ(40, row[1].asInt());
    EXPECT_EQ(0, row[2].asInt());
    row = res_rows->next();
    EXPECT_EQ(20, row[0].asInt());
    EXPECT_EQ(40, row[1].asInt());
    EXPECT_EQ(1, row[2].asInt());
    row = res_rows->next();
    EXPECT_EQ(30, row[0].asInt());
    EXPECT_EQ(60, row[1].asInt());
    EXPECT_EQ(2, row[2].asInt());
    row = res_rows->next();
    EXPECT_EQ(30, row[0].asInt());
    EXPECT_EQ(60, row[1].asInt());
    EXPECT_EQ(3, row[2].asInt());
    row = res_rows->next();
    EXPECT_EQ(30, row[0].asInt());
    EXPECT_EQ(60, row[1].asInt());
    EXPECT_EQ(4, row[2].asInt());


    EXPECT_EQ(5, block2->size());
    auto res_rows2 = block2->rows();
    row = res_rows2->next();
    EXPECT_EQ(120, row[0].asInt());
    EXPECT_EQ(240, row[1].asInt());
    EXPECT_EQ(5, row[2].asInt());
    row = res_rows2->next();
    EXPECT_EQ(120, row[0].asInt());
    EXPECT_EQ(240, row[1].asInt());
    EXPECT_EQ(6, row[2].asInt());
    row = res_rows2->next();
    EXPECT_EQ(140, row[0].asInt());
    EXPECT_EQ(280, row[1].asInt());
    EXPECT_EQ(7, row[2].asInt());
    row = res_rows2->next();
    EXPECT_EQ(140, row[0].asInt());
    EXPECT_EQ(280, row[1].asInt());
    EXPECT_EQ(8, row[2].asInt());
    row = res_rows2->next();
    EXPECT_EQ(140, row[0].asInt());
    EXPECT_EQ(280, row[1].asInt());
    EXPECT_EQ(9, row[2].asInt());
}