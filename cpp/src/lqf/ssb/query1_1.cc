//
// Created by Harper on 1/2/21.
//
#include "ssbquery.h"
#include "query1.h"

namespace lqf {
    using namespace agg;
    namespace ssb {
        namespace q1_1 {
            int discount_from = 1;
            int discount_to = 3;
            int quantity_upper = 25;
            ByteArray year_from("19940101");
            ByteArray year_to("19941231");
        }

        using namespace sboost;
        using namespace q1;
        using namespace q1_1;

        // This impl uses a scan to replace the date table join.
        void executeQ1_1Plain() {
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::ORDERDATE, LineOrder::DISCOUNT, LineOrder::QUANTITY,
                                                      LineOrder::EXTENDEDPRICE});

            ColFilter colFilter(
                    {new SboostPredicate<Int32Type>(LineOrder::DISCOUNT,
                                                    bind(Int32DictBetween::build, discount_from, discount_to)),
                     new SboostPredicate<Int32Type>(LineOrder::QUANTITY,
                                                    bind(Int32DictLess::build, quantity_upper)),
                     new SboostPredicate<ByteArrayType>(LineOrder::ORDERDATE,
                                                        bind(ByteArrayDictBetween::build, year_from, year_to))
                    });
            auto filtered = colFilter.filter(*lineorderTable);

            SimpleAgg agg([]() { return vector<AggField *>({new RevenueField()}); });
            auto agged = agg.agg(*filtered);

            Printer printer(PBEGIN PD(0) PEND);
            printer.print(*agged);
        }

        void executeQ1_1() {

        }
    }
}