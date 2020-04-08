//
// Created by harper on 2/24/20.
//

#include <gtest/gtest.h>
#include "agg.h"

using namespace std;
using namespace lqf;
using namespace lqf::agg;

TEST(AggFieldTest, Max) {

    vector<uint32_t> offset({0, 1, 2});

    MemDataRow storage(offset);

    MemDataRow row(offset);

    DoubleMax max1(0);
    max1.storage_ = storage[0].data();
    max1.init();
    for (int i = 0; i < 50; ++i) {
        row[0] = i % 20;
        max1.reduce(row);
    }
    EXPECT_EQ(19, max1.storage_.asInt());

    DoubleMax max2(0);
    max2.storage_ = storage[1].data();
    max2.init();
    for (int i = 0; i < 50; ++i) {
        row[0] = i % 30;
        max2.reduce(row);
    }
    EXPECT_EQ(29, max2.storage_.asInt());

    max1.merge(max2);
    EXPECT_EQ(29, max1.storage_.asInt());
}

TEST(AggFieldTest, Min) {

    vector<uint32_t> offset({0, 1, 2});

    MemDataRow storage(offset);

    MemDataRow row(offset);

    DoubleMin min1(0);
    min1.storage_ = storage[0].data();
    min1.init();
    for (int i = 0; i < 50; ++i) {
        row[0] = (i % 20) - 40;
        min1.reduce(row);
    }
    EXPECT_EQ(-21, min1.storage_.asInt());

    DoubleMax min2(0);
    min2.storage_ = storage[1].data();
    min2.init();
    for (int i = 0; i < 50; ++i) {
        row[0] = i % 30 - 40;
        min2.reduce(row);
    }
    EXPECT_EQ(-11, min2.storage_.asInt());

    min1.merge(min2);
    EXPECT_EQ(-21, min1.storage_.asInt());
}

TEST(HashAggTest, Agg) {
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };

    const vector<uint32_t> &col_size = lqf::colSize(3);
    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>({new DoubleSum(1), new Count()});
    };

    HashAgg agg(col_size, {AGI(0)}, aggFields, hasher);

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    for (int i = 0; i < 100; i++) {
        (*row1)[i][0] = i / 10;
        (*row2)[i][0] = (i + 100) / 10;
        (*row1)[i][1] = i / 10 * 0.1;
        (*row2)[i][1] = (i + 100) / 10 * 0.1;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(20, aggblock->size());
    auto rows = aggblock->rows();
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(10, (*rows)[i][2].asInt());
        auto val = (*rows)[i][0].asInt();
        auto sum = (*rows)[i][1].asDouble();
        EXPECT_DOUBLE_EQ(val, sum);
    }
}

TEST(HashAggTest, AggRecording) {
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };

    HashAgg agg(lqf::colSize(3), {AGI(0)}, []() {
        return vector<AggField *>{new IntRecordingMin(1, 2)};
    }, hasher);
    agg.useRecording();

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    for (int i = 0; i < 100; i++) {
        (*row1)[i][0] = 0;
        (*row1)[i][2] = i;
        (*row1)[i][1] = i % 30;
        (*row2)[i][0] = 1;
        (*row2)[i][2] = i;
        (*row2)[i][1] = (i % 40) + 5;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(7, aggblock->size());
    auto rows = aggblock->rows();

    EXPECT_EQ((*rows)[0][0].asInt(), 1);
    EXPECT_EQ((*rows)[0][1].asInt(), 80);
    EXPECT_EQ((*rows)[0][2].asInt(), 5);
    EXPECT_EQ((*rows)[1][0].asInt(), 1);
    EXPECT_EQ((*rows)[1][1].asInt(), 40);
    EXPECT_EQ((*rows)[1][2].asInt(), 5);
    EXPECT_EQ((*rows)[2][0].asInt(), 1);
    EXPECT_EQ((*rows)[2][1].asInt(), 0);
    EXPECT_EQ((*rows)[2][2].asInt(), 5);

    EXPECT_EQ((*rows)[3][0].asInt(), 0);
    EXPECT_EQ((*rows)[3][1].asInt(), 90);
    EXPECT_EQ((*rows)[3][2].asInt(), 0);
    EXPECT_EQ((*rows)[4][0].asInt(), 0);
    EXPECT_EQ((*rows)[4][1].asInt(), 60);
    EXPECT_EQ((*rows)[4][2].asInt(), 0);
    EXPECT_EQ((*rows)[5][0].asInt(), 0);
    EXPECT_EQ((*rows)[5][1].asInt(), 30);
    EXPECT_EQ((*rows)[5][2].asInt(), 0);
    EXPECT_EQ((*rows)[6][0].asInt(), 0);
    EXPECT_EQ((*rows)[6][1].asInt(), 0);
    EXPECT_EQ((*rows)[6][2].asInt(), 0);

}

TEST(TableAggTest, Agg) {
    function<uint32_t(DataRow &)> indexer =
            [](DataRow &row) {
                return row[0].asInt();
            };

    TableAgg agg(lqf::colSize(3), {AGI(0)},
                 []() { return vector<AggField *>{new DoubleSum(1), new Count()}; }, 10, indexer);

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(1000);
    auto block2 = memTable->allocate(1000);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    for (int i = 0; i < 1000; i++) {
        (*row1)[i][0] = i % 10;
        (*row2)[i][0] = i % 10;
        (*row1)[i][1] = i % 10 * 0.1;
        (*row2)[i][1] = i % 10 * 0.1;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(10, aggblock->size());
    auto rows = aggblock->rows();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(200, (*rows)[i][2].asInt());

        EXPECT_NEAR(i * 20, (*rows)[i][1].asDouble(), 0.001);
    }
}

TEST(SimpleAggTest, Agg) {
    SimpleAgg agg(lqf::colSize(2), []() { return vector<AggField *>{new DoubleSum(1), new Count()}; });

    auto memTable = MemTable::Make(3);

    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();

    for (int i = 0; i < 100; i++) {
        (*row1)[i][0] = i / 10;
        (*row2)[i][0] = (i + 100) / 10;
        (*row1)[i][1] = i / 10 * 0.1;
        (*row2)[i][1] = (i + 100) / 10 * 0.1;
    }

    auto aggtable = agg.agg(*memTable);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(1, aggblock->size());
    auto rows = aggblock->rows();
    EXPECT_DOUBLE_EQ(190, (*rows)[0][0].asDouble());
    EXPECT_EQ(200, (*rows)[0][1].asInt());
}

TEST(HashDictAggTest, Agg) {
    vector<uint32_t> col_size({2, 1, 1});
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row(14).asInt();
            };
    HashDictAgg agg(col_size, {AGR(14)}, []() {
        return vector<AggField *>({new DoubleSum(5), new Count()});
    }, hasher, {{14, 0}});

    auto table = ParquetTable::Open("lineitem", (1 << 15) - 1);

    auto aggtable = agg.agg(*table);
    auto agged = aggtable->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(7, aggblock->size());
    auto rows = aggblock->rows();

    EXPECT_EQ(ByteArray("MAIL"), (*rows)[0][0].asByteArray());
    EXPECT_NEAR(21016140.9999, (*rows)[0][1].asDouble(), 0.0001);
    EXPECT_EQ(824, (*rows)[0][2].asInt());

    EXPECT_EQ(ByteArray("SHIP"), (*rows)[1][0].asByteArray());
    EXPECT_NEAR(21007607.8100, (*rows)[1][1].asDouble(), 0.0001);
    EXPECT_EQ(828, (*rows)[1][2].asInt());

    EXPECT_EQ(ByteArray("AIR"), (*rows)[2][0].asByteArray());
    EXPECT_NEAR(20903956.200000, (*rows)[2][1].asDouble(), 0.0001);
    EXPECT_EQ(838, (*rows)[2][2].asInt());

    EXPECT_EQ(ByteArray("FOB"), (*rows)[3][0].asByteArray());
    EXPECT_NEAR(21932124.379999, (*rows)[3][1].asDouble(), 0.0001);
    EXPECT_EQ(865, (*rows)[3][2].asInt());

    EXPECT_EQ(ByteArray("RAIL"), (*rows)[4][0].asByteArray());
    EXPECT_NEAR(22436302.55000, (*rows)[4][1].asDouble(), 0.0001);
    EXPECT_EQ(868, (*rows)[4][2].asInt());

    EXPECT_EQ(ByteArray("REG AIR"), (*rows)[5][0].asByteArray());
    EXPECT_NEAR(22130849.4199999, (*rows)[5][1].asDouble(), 0.0001);
    EXPECT_EQ(879, (*rows)[5][2].asInt());

    EXPECT_EQ(ByteArray("TRUCK"), (*rows)[6][0].asByteArray());
    EXPECT_NEAR(23347417.020000, (*rows)[6][1].asDouble(), 0.0001);
    EXPECT_EQ(903, (*rows)[6][2].asInt());
}