//
// Created by Harper on 1/2/21.
//
#include "query1.h"
#include <iostream>

namespace lqf {
    using namespace agg;
    namespace ssb {
        namespace q1_1 {
            double discount_from = 1;
            double discount_to = 3;
            int quantity_upper = 25;
            ByteArray year_from("19940101");
            ByteArray year_to("19941231");
        }

        using namespace sboost;
        using namespace parallel;
        using namespace q1;
        using namespace q1_1;

        // This impl uses a scan to replace the date table join.
        void executeQ1_1Plain() {
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::ORDERDATE, LineOrder::DISCOUNT, LineOrder::QUANTITY,
                                                      LineOrder::EXTENDEDPRICE});


            ColFilter colFilter(
                    {/*new SboostPredicate<DoubleType>(LineOrder::DISCOUNT,
                                                     bind(DoubleDictBetween::build, discount_from, discount_to)),*/
                     new SboostPredicate<Int32Type>(LineOrder::QUANTITY,
                                                    bind(Int32DictLess::build, quantity_upper)),
                     new SboostPredicate<ByteArrayType>(LineOrder::ORDERDATE,
                                                        bind(ByteArrayDictBetween::build, year_from, year_to))
                    });
            auto filtered = colFilter.filter(*lineorderTable);

            cout << filtered->size()<<'\n';
//            SimpleAgg agg([]() { return vector<AggField *>({new RevenueField()}); });
//            auto agged = agg.agg(*filtered);
//
//            Printer printer(PBEGIN PD(0) PEND);
//            printer.print(*agged);
        }

        void executeQ1_1() {
            ExecutionGraph graph;

            auto lineorder = ParquetTable::Open(LineOrder::path,
                                                {LineOrder::ORDERDATE, LineOrder::DISCOUNT, LineOrder::QUANTITY,
                                                 LineOrder::EXTENDEDPRICE});

            auto lineorderTable = graph.add(new TableNode(lineorder), {});

            auto colFilter = graph.add(new ColFilter(
                    {new SboostPredicate<DoubleType>(LineOrder::DISCOUNT,
                                                     bind(DoubleDictBetween::build, discount_from, discount_to)),
                     new SboostPredicate<Int32Type>(LineOrder::QUANTITY,
                                                    bind(Int32DictLess::build, quantity_upper)),
                     new SboostPredicate<ByteArrayType>(LineOrder::ORDERDATE,
                                                        bind(ByteArrayDictBetween::build, year_from, year_to))
                    }), {lineorderTable});

            auto agg = graph.add(new SimpleAgg([]() { return vector<AggField *>({new RevenueField()}); }), {colFilter});

            graph.add(new Printer(PBEGIN PD(0) PEND), {agg});
            graph.execute(true);
        }
    }
}