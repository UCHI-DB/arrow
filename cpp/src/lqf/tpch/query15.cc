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
        using namespace agg;
        using namespace sboost;

        namespace q15 {

            ByteArray dateFrom("1996-01-01");
            ByteArray dateTo("1996-04-01");

            class PriceField : public DoubleSum {
            public:
                PriceField() : DoubleSum(0) {}

                void reduce(DataRow &input) override {
                    *value_ += input[LineItem::EXTENDEDPRICE].asDouble() * (1 - input[LineItem::DISCOUNT].asDouble());
                }
            };
        }
        using namespace q15;

        void executeQ15() {

            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SUPPKEY, LineItem::SHIPDATE, LineItem::EXTENDEDPRICE,
                                                LineItem::DISCOUNT});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NAME, Supplier::ADDRESS,
                                                                Supplier::PHONE});

            ColFilter lineitemDateFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                             bind(&ByteArrayDictRangele::build,
                                                                                  dateFrom, dateTo))});
            auto filteredLineitem = lineitemDateFilter.filter(*lineitem);


            HashAgg suppkeyAgg(vector<uint32_t>({1, 1}), {AGI(LineItem::SUPPKEY)},
                               []() { return vector<AggField *>{new PriceField()}; },
                               COL_HASHER(LineItem::SUPPKEY));
            // SUPPKEY, REVENUE
            auto revenueView = suppkeyAgg.agg(*filteredLineitem);

            SimpleAgg maxAgg(vector<uint32_t>{1, 1},
                             []() { return vector<AggField *>{new DoubleRecordingMax(1, 0)}; });
            maxAgg.useRecording();
            // SUPPKEY, REV
            auto maxRevenue = maxAgg.agg(*revenueView);

            HashJoin join(Supplier::SUPPKEY, 0, new RowBuilder({JLS(Supplier::NAME), JLS(Supplier::ADDRESS),
                                                                JLS(Supplier::PHONE), JR(1)}, true, false));
            auto supplierWithRev = join.join(*supplier, *maxRevenue);

            SmallSort sort([](DataRow *a, DataRow *b) { return SILE(0); });
            auto sorted = sort.sort(*supplierWithRev);

            Printer printer(PBEGIN PI(0) PB(1) PB(2) PB(3) PD(4) PEND);
            printer.print(*sorted);
        }
    }
}