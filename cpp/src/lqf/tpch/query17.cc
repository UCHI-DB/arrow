//
// Created by harper on 4/6/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {

        void executeQ17() {
            ByteArray brand("Brand#23");
            ByteArray container("MED BOX");

            auto part = ParquetTable::Open(Part::path, {Part::BRAND, Part::CONTAINER, Part::PARTKEY});
            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::PARTKEY});

            using namespace sboost;
            ColFilter partFilter({new SboostPredicate<ByteArrayType>(Part::BRAND,
                                                                     bind(&ByteArrayDictEq::build, brand)),
                                  new SboostPredicate<ByteArrayType>(Part::CONTAINER,
                                                                     bind(&ByteArrayDictEq::build, container))});
            auto validPart = partFilter.filter(*part);

            HashFilterJoin lineitemFilter(LineItem::PARTKEY, Part::PARTKEY);
            auto validlineitem = FilterMat().mat(*lineitemFilter.join(*lineitem, *validPart));

            using namespace agg;
            HashAgg avgquantity(vector<uint32_t>({1, 1}), {AGI(LineItem::PARTKEY)},
                                []() { return vector<AggField *>{new IntAvg(LineItem::QUANTITY)}; },
                                COL_HASHER(LineItem::PARTKEY));
            // PARTKEY, AVG_QTY
            auto aggedlineitem = avgquantity.agg(*validlineitem);

            HashJoin withAvgJoin(LineItem::PARTKEY, 0, new RowBuilder({JL(LineItem::EXTENDEDPRICE)}),
                                 [](DataRow &left, DataRow &right) {
                                     return left[LineItem::QUANTITY].asInt() < 0.2 * right[1].asDouble();
                                 });
            auto result = withAvgJoin.join(*validlineitem, *aggedlineitem);

            class YearSumField : public DoubleSum {
            public:
                YearSumField() : DoubleSum(0) {}

                void dump() override {
                    *value_ /= 7;
                }
            };
            SimpleAgg sumagg(vector<uint32_t>({1}), []() { return vector<AggField *>{new YearSumField()}; });
            result = sumagg.agg(*result);

            auto printer = Printer::Make(PBEGIN PD(0) PEND);
            printer->print(*result);
        }
    }
}