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
        using namespace agg;
        namespace q3 {
            ByteArray segment("HOUSEHOLD");
            ByteArray date("1995-03-15");

            class PriceField : public DoubleSum {
            public:
                PriceField() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    *value_ += dataRow[1].asDouble() * (1 - dataRow[2].asDouble());
                }
            };
        }
        using namespace sboost;
        using namespace q3;

        void executeQ3() {

            auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY});
            auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY, Orders::ORDERDATE,
                                                                Orders::SHIPPRIORITY});
            auto lineItemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::ORDERKEY, LineItem::EXTENDEDPRICE, LineItem::DISCOUNT});

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
                    {JL(LineItem::EXTENDEDPRICE), JL(LineItem::DISCOUNT), JRR(Orders::ORDERDATE),
                     JRR(Orders::SHIPPRIORITY)}, true));
            // ORDERKEY EXTENDEDPRICE DISCOUNT ORDERDATE SHIPPRIORITY
            auto orderItemTable = orderItemJoin.join(*filteredLineItemTable, *filteredOrderTable);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                int orderkey = data[0].asInt();
                int shipdate = data[3].asInt();
                int priority = data[4].asInt();
                uint64_t key = 0;
                key += static_cast<uint64_t>(orderkey) << 32;
                key += shipdate << 5;
                key += priority;
                return key;
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new PriceField()};
            };
            HashAgg orderItemAgg(vector<uint32_t>{1, 1, 1, 1}, {AGI(0), AGI(3), AGI(4)}, aggFields, hasher);
//            orderItemAgg.useVertical();
            auto result = orderItemAgg.agg(*orderItemTable);

            auto comparator = [](DataRow *a, DataRow *b) {
                return SDGE(3) || (SDE(3) && SILE(2));
            };
            TopN sort(10, comparator);
            result = sort.sort(*result);

            auto orderdateDict = orderTable->LoadDictionary<ByteArrayType>(Orders::ORDERDATE);
            auto shippriorityDict = orderTable->LoadDictionary<ByteArrayType>(Orders::SHIPPRIORITY);
            auto oddictp = orderdateDict.get();
            auto spdictp = shippriorityDict.get();
            Printer printer(PBEGIN PI(0) PDICT(oddictp, 1) PDICT(spdictp,2) PD(3) PEND);

            printer.print(*result);
        }
    }
}