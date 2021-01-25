//
// Created by Harper on 1/3/21.
//

#ifndef ARROW_SSB_QUERY3_H
#define ARROW_SSB_QUERY3_H

#include "../data_model.h"
#include "../filter.h"
#include "../agg.h"
#include "../join.h"
#include "../tjoin.h"
#include "../print.h"
#include "../sort.h"
#include "ssbquery.h"

namespace lqf {
    namespace ssb {
        namespace q3 {

            class WithCityBuilder : public RowBuilder {
            public:
                WithCityBuilder() : RowBuilder(
                        {JL(LineOrder::CUSTKEY), JRR(Supplier::CITY),
                         JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = left[LineOrder::CUSTKEY].asInt();
                    target[1] = right[0].asInt();
                    target[2] = udf::date2year(left[LineOrder::ORDERDATE].asByteArray());
                    target[3] = left[LineOrder::REVENUE].asDouble();
                }
            };

            class WithCityColBuilder : public ColumnBuilder {
            public:
                WithCityColBuilder() : ColumnBuilder({JL(LineOrder::CUSTKEY), JRR(Supplier::CITY),
                                                        JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE)}) {}

                shared_ptr<MemvBlock> cacheToMem(Block &input) override {
                    auto block_size = input.size();
                    auto memcache = make_shared<MemvBlock>(block_size, load_col_size_);

                    copyColumn(block_size, *memcache, 0, input, LineOrder::CUSTKEY);

                    // Copy year
                    auto reader = input.col(LineOrder::ORDERDATE);
                    auto writer = memcache->col(1);
                    for (uint32_t j = 0; j < block_size; ++j) {
                        (*writer)[j] = udf::date2year(reader->next().asByteArray());
                    }

                    copyColumn(block_size, *memcache, 2, input, LineOrder::REVENUE);

                    return memcache;
                }
            };

        }
    }
}
#endif //ARROW_SSB_QUERY3_H
