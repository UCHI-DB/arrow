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
        namespace q18 {

            int quantity = 300;
        }

        using namespace q18;

        void executeQ18() {

            auto order = ParquetTable::Open(Orders::path,
                                            {Orders::ORDERKEY, Orders::ORDERDATE, Orders::TOTALPRICE, Orders::CUSTKEY});
            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::ORDERKEY, LineItem::QUANTITY});
            auto customer = ParquetTable::Open(Customer::path, {Customer::NAME, Customer::CUSTKEY});

            HashAgg hashAgg(vector<uint32_t>{1, 1}, {AGI(LineItem::ORDERKEY)},
                            []() { return vector<AggField *>{new IntSum(LineItem::QUANTITY)}; },
                            COL_HASHER(LineItem::ORDERKEY));
            hashAgg.setPredicate([=](DataRow &row) { return row[1].asInt() > quantity; });
            // ORDERKEY, SUM_QUANTITY
            auto validLineitem = hashAgg.agg(*lineitem);

            HashJoin withOrderJoin(Orders::ORDERKEY, 0,
                                   new RowBuilder(
                                           {JL(Orders::CUSTKEY), JLS(Orders::ORDERDATE), JL(Orders::TOTALPRICE), JR(1)},
                                           true, true));
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY
            auto withOrder = withOrderJoin.join(*order, *validLineitem);

            HashColumnJoin withCustomerJoin(1, Customer::CUSTKEY,
                                            new ColumnBuilder(
                                                    {JL(0), JL(1), JLS(2), JL(3), JL(4), JRS(Customer::NAME)}));
            // ORDERKEY, CUSTKEY, ORDERDATE, TOTALPRICE, SUM_QUANTITY,CUSTNAME
            auto withCustomer = withCustomerJoin.join(*withOrder, *customer);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SDGE(3) || (SDE(3) && SBLE(2));
            };
            TopN topn(100, comparator);
            SmallSort sort(comparator);
            auto sorted = sort.sort(*withCustomer);

            Printer printer(PBEGIN PB(5) PI(1) PI(0) PB(2) PD(3) PI(4) PEND);
            printer.print(*sorted);
        }
    }
}