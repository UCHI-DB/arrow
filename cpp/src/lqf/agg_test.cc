//
// Created by harper on 2/24/20.
//

#include <gtest/gtest.h>
#include "agg.h"

using namespace std;
using namespace lqf;
using namespace lqf::agg;

TEST(HashAggTest, Agg) {
    function<uint64_t(DataRow &)> hasher =
            [](DataRow &row) {
                return row[0].asInt();
            };
    function<unique_ptr<AggReducer>(DataRow &)> headerInit =
            [](DataRow &row) {
                unique_ptr<AggReducer> reducer = unique_ptr<AggReducer>(
                        new AggReducer(1, {new DoubleSum(1), new Count()}));
                reducer->header()[0] = row[0].asInt();
                return reducer;
            };
    function<shared_ptr<HashCore>()> maker = [=]() {
        return make_shared<HashCore>(3, hasher, headerInit);
    };

    HashAgg agg(maker);

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

    auto agged = agg.agg(*memTable)->blocks()->collect();

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

TEST(TableAggTest, Agg) {
    function<uint32_t(DataRow &)> indexer =
            [](DataRow &row) {
                return row[0].asInt();
            };
    function<unique_ptr<AggReducer>(DataRow &)> headerInit =
            [](DataRow &row) {
                unique_ptr<AggReducer> reducer = unique_ptr<AggReducer>(
                        new AggReducer(1, {new DoubleSum(1), new Count()}));
                reducer->header()[0] = row[0].asInt();
                return reducer;
            };
    function<shared_ptr<TableCore>()> maker = [=]() {
        return make_shared<TableCore>(3, 10, indexer, headerInit);
    };

    TableAgg agg(maker);

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

    auto agged = agg.agg(*memTable)->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(10, aggblock->size());
    auto rows = aggblock->rows();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(200, (*rows)[i][2].asInt());

        EXPECT_NEAR(i * 20, (*rows)[i][1].asDouble(),0.001);
    }
}

TEST(SimpleAggTest, Agg) {
    function<unique_ptr<AggReducer>(DataRow &)> headerInit =
            [](DataRow &row) {
                unique_ptr<AggReducer> reducer = unique_ptr<AggReducer>(
                        new AggReducer(0, {new DoubleSum(1), new Count()}));
                return reducer;
            };
    function<shared_ptr<SimpleCore>()> maker = [=]() {
        return make_shared<SimpleCore>(2, headerInit);
    };

    SimpleAgg agg(maker);

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

    auto agged = agg.agg(*memTable)->blocks()->collect();

    EXPECT_EQ(1, agged->size());
    auto aggblock = (*agged)[0];
    EXPECT_EQ(1, aggblock->size());
    auto rows = aggblock->rows();
    EXPECT_DOUBLE_EQ(190, (*rows)[0][0].asDouble());
    EXPECT_EQ(200, (*rows)[0][1].asInt());
}