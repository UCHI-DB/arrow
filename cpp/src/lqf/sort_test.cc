//
// Created by harper on 2/27/20.
//
#include <cstdlib>
#include <gtest/gtest.h>
#include "sort.h"

using namespace lqf;

TEST(TopNTest, Sort) {
    srand(time(NULL));

    auto table = MemTable::Make(4);
    auto block = table->allocate(1000);

    vector<int> buffer;

    auto rows = block->rows();
    for (int i = 0; i < 1000; i++) {
        int next = rand();
        buffer.push_back(next);
        (*rows)[i][0] = next;
    }

    auto top10 = TopN(10, 4, [](DataRow *a, DataRow *b) {
        return (*a)[0].asInt() < (*b)[0].asInt();
    });

    auto sorted = top10.sort(*table);
    auto sortedblock = (*sorted->blocks()->collect())[0];
    EXPECT_EQ(10, sortedblock->size());

    sort(buffer.begin(), buffer.end());
    auto sortedrows = sortedblock->rows();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(buffer[i], (*sortedrows)[i][0].asInt());
    }
}