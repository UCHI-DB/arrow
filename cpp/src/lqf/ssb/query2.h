//
// Created by Harper on 1/2/21.
//

#ifndef ARROW_SSB_QUERY2_H
#define ARROW_SSB_QUERY2_H

#include "../data_model.h"
#include "../filter.h"
#include "../agg.h"
#include "../join.h"
#include "../print.h"
#include "../sort.h"
#include "../operator_enc.h"
#include "ssbquery.h"

namespace lqf {
    namespace ssb {
        namespace q2 {
            class Q2RowBuilder : public RowBuilder {
            public:
                Q2RowBuilder() : RowBuilder(
                        {JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE), JRR(Part::BRAND)}, false, false) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = left[LineOrder::REVENUE].asDouble();
                    target[1] = udf::date2year(left[LineOrder::ORDERDATE].asByteArray());
                    target[2] = right(0).asInt();
                }
            };
        }
    }
}

#endif //ARROW_SSB_QUERY2_H
