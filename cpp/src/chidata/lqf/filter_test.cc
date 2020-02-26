//
// Created by harper on 2/24/20.
//

#include <gtest/gtest.h>
#include "filter.h"
#include <chidata/lqf/data_model.h>

using namespace chidata::lqf;

TEST(RowFilterTest, Filter) {
    auto memTable = MemTable::Make(5);
    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();
    for (int i = 0; i < 100; ++i) {
        (*row1)[i][0] = i;
        (*row1)[i][1] = static_cast<double>((i - 50) * (i - 50) * 0.05);
        (*row2)[i][0] = (i + 3) * i + 1;
    }

    function<bool(DataRow &)> pred = [](DataRow &row) {
        return row[0].asInt() % 5 == 0;
    };

    auto rowFilter = RowFilter(pred);
    auto filteredTable = rowFilter.filter(*memTable);

    auto filteredBlocks = filteredTable->blocks()->collect();

    auto fblock1 = (*filteredBlocks)[0];
    auto fblock2 = (*filteredBlocks)[1];

    EXPECT_EQ(20, fblock1->size());
    EXPECT_EQ(20, fblock2->size());
}


TEST(ColFilterTest, Filter) {

}