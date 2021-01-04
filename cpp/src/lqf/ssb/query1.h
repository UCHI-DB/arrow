//
// Created by Harper on 1/2/21.
//

#ifndef ARROW_QUERY1_H
#define ARROW_QUERY1_H

#include "../agg.h"
#include "../data_model.h"
#include "../filter.h"
#include "../print.h"

namespace lqf {
    using namespace agg;
    namespace ssb {
        namespace q1 {
            class RevenueField : public DoubleSum {
            public:
                RevenueField() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ = value_.asDouble() + dataRow[LineOrder::EXTENDEDPRICE].asDouble()
                                                 * dataRow[LineOrder::DISCOUNT].asDouble();
                }
            };
        }
    }
}
#endif //ARROW_QUERY1_H
