//
// Created by harper on 3/3/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include "tpchquery.h"

namespace lqf {
    namespace tpch {

        using namespace lqf::sboost;
        using namespace lqf::agg;

        void executeQ3() {
            ByteArray segment("HOUSEHOLD");
            ByteArray date("1995-03-15");

            auto customerTable = ParquetTable::Open(Customer::path);
            auto orderTable = ParquetTable::Open(Orders::path);
            auto lineItemTable = ParquetTable::Open(LineItem::path);

            ColFilter custFilter(
                    {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
            auto filteredCustTable = custFilter.filter(*customerTable);
//
            ColFilter orderFilter({new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                      bind(&ByteArrayDictLess::build, date))});
            auto filteredOrderTable = orderFilter.filter(*orderTable);

            HashFilterJoin orderOnCustFilterJoin(Orders::CUSTKEY, Customer::CUSTKEY);
            filteredOrderTable = orderOnCustFilterJoin.join(*filteredOrderTable, *filteredCustTable);

            // TODO Which is faster? First filter lineitem then join, or filter join then filter

            ColFilter lineItemFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                         bind(&ByteArrayDictGreater::build, date))});
            auto filteredLineItemTable = lineItemFilter.filter(*lineItemTable);

            HashJoin orderItemJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new RowBuilder(
                    {JL(LineItem::EXTENDEDPRICE), JL(LineItem::DISCOUNT), JRS(Orders::ORDERDATE),
                     JRS(Orders::SHIPPRIORITY)}, true));

            auto orderItemTable = orderItemJoin.join(*filteredLineItemTable, *filteredOrderTable);

            class Field : public DoubleSum {
            public:
                Field() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ += dataRow[1].asDouble() * (1 - dataRow[2].asDouble());
                }
            };

            HashAgg orderItemAgg(4, []() {
                return unique_ptr<HashCore>(
                        new HashCore([](DataRow &data) {
                                         int orderkey = data[0].asInt();
                                         int shipdate = data(3).asInt();
                                         int priority = data(4).asInt();
                                         uint64_t key = 0;
                                         key += static_cast<uint64_t>(orderkey) << 32;
                                         key += shipdate << 5;
                                         key += priority;
                                         return key;
                                     },
                                     [](DataRow &source) {
                                         auto reducer = unique_ptr<AggReducer>(
                                                 new AggReducer(3, {new Field()}));
                                         reducer->header()[0] = source[0];
                                         reducer->header()[1] = source[3];
                                         reducer->header()[2] = source[4];

                                         return reducer;
                                     }));
            });

            auto result = orderItemAgg.agg(*orderItemTable);

            auto comparator = [](DataRow *a, DataRow *b) {
                return SDGE(1) || (SDE(1) && SBLE(2));
            };
            TopN sort(10, comparator);
            result = sort.sort(*result);

            auto printer = Printer::Make(PBEGIN PI(0) PB(1) PB(2) PD(3) PEND);
            printer->print(*result);
        }
    }
}