//
// Created by harper on 2/9/21.
//

#include <gtest/gtest.h>
#include <iostream>
#include "operator_enc.h"
#include "data_model.h"

using namespace std;
using namespace lqf;
using namespace lqf::encoding;
using namespace lqf::encopr;

TEST(EncMatBetweenFilter, Filter) {
    auto memtable = MemTable::Make(3);

    auto block = shared_ptr<EncMemvBlock>(new EncMemvBlock({parquet::Type::type::INT32, parquet::Type::type::INT32},
                                                           {EncodingType::PLAIN, EncodingType::BITPACK}));

    auto writer = block->rows();
    for (int i = 0; i < 10000; i++) {
        DataRow &row = writer->next();
        row[0] = i;
        row[1] = i % 10;
    }

    writer->close();

    EXPECT_EQ(block->size(), 10000);

    memtable->append(block);

    EncMatBetweenFilter filter({parquet::Type::type::INT32, parquet::Type::type::INT32},
                               {EncodingType::PLAIN, EncodingType::PLAIN}, 1, 3, 6);
    auto filtered = filter.filter(*memtable);

    auto blocks = filtered->blocks()->collect();
    auto res0 = (*blocks)[0];
    EXPECT_EQ(4000, res0->size());

    auto reader = res0->rows();
    for (int i = 0; i < 4000; ++i) {
        DataRow &row = reader->next();
        auto val = row[1].asInt();
        ASSERT_EQ(val, i % 4 + 3);
        ASSERT_EQ(row[0].asInt(), i / 4 * 10 + val);
    }

}