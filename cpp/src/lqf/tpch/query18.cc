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

        void executeQ18() {
            int quantity = 300;

            auto order = ParquetTable::Open(Orders::path,
                                            {Orders::ORDERKEY, Orders::ORDERDATE, Orders::TOTALPRICE, Orders::CUSTKEY});
            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::ORDERKEY, LineItem::QUANTITY});
            auto customer = ParquetTable::Open(Customer::path, {Customer::NAME, Customer::CUSTKEY});

            using namespace agg;
            HashAgg hashAgg(vector<uint32_t>{1, 1}, {AGI(LineItem::ORDERKEY)},
                            []() { return vector<AggField *>{new IntSum(LineItem::QUANTITY)}; },
                            COL_HASHER(LineItem::ORDERKEY));
            auto aggedlineitem = hashAgg.agg(*lineitem);

            RowFilter filter([](DataRow &row) { return row[1].asInt() > 300; });
            // ORDERKEY, SUM_QUANTITY
            auto validOrders = filter.filter(*aggedlineitem);

            HashJoin withOrderJoin(Orders::ORDERKEY, 0,
                                   new RowBuilder(
                                           {JL(Orders::CUSTKEY), JLS(Orders::ORDERDATE), JL(Orders::TOTALPRICE), JR(1)},
                                           true, true));
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY
            auto result = withOrderJoin.join(*order, *validOrders);

            HashColumnJoin withCustomerJoin(1, Customer::CUSTKEY,
                                            new ColumnBuilder(
                                                    {JL(0), JL(1), JL(2), JL(3), JL(4), JRS(Customer::NAME)}));
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY,CUSTNAME
            result = withCustomerJoin.join(*result, *customer);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SDLE(3) || (SDE(3) && SBLE(2));
            };
            TopN topn(100, comparator);
            result = topn.sort(*result);

            auto printer = Printer::Make(PBEGIN PI(0) PI(1) PB(2) PD(3) PI(4) PB(5) PEND);
            printer->print(*result);
        }
    }
}