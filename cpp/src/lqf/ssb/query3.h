//
// Created by Harper on 1/3/21.
//

#ifndef ARROW_SSB_QUERY3_H
#define ARROW_SSB_QUERY3_H

#include "../data_model.h"
#include "../filter.h"
#include "../agg.h"
#include "../join.h"
#include "../print.h"
#include "../sort.h"
#include "ssbquery.h"

namespace lqf {
    namespace ssb {
        namespace q3 {
            class WithNationBuilder : public RowBuilder {
            public:
                WithNationBuilder() : RowBuilder(
                        {JL(LineOrder::CUSTKEY), JRR(Supplier::NATION),
                         JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = left[LineOrder::CUSTKEY].asInt();
                    target[1] = right[0].asInt();
                    target[2] = udf::date2year(left[LineOrder::ORDERDATE].asByteArray());
                    target[3] = left[LineOrder::REVENUE].asDouble();
                }
            };

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
        }
    }
}
#endif //ARROW_SSB_QUERY3_H
