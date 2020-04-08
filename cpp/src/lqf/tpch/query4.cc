//
// Created by harper on 4/2/20.
//

#include <lqf/filter.h>
#include <lqf/data_model.h>
#include <parquet/types.h>
#include <lqf/mat.h>
#include <lqf/join.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include "tpchquery.h"

namespace lqf {
    namespace tpch {

        using namespace lqf::sboost;

        void executeQ4() {
            ByteArray dateFrom("1993-07-01");
            ByteArray dateTo("1993-10-01");

            auto orderTable = ParquetTable::Open(Orders::path);
            auto lineitemTable = ParquetTable::Open(LineItem::path);

            ColFilter orderFilter(
                    {new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                        bind(&ByteArrayDictRangele::build, dateFrom, dateTo))});
            FilterMat filterMat;
            auto filteredOrderTable = filterMat.mat(*orderFilter.filter(*orderTable));

            RowFilter lineItemFilter([](DataRow &datarow) {
                return datarow[LineItem::COMMITDATE].asByteArray() < datarow[LineItem::RECEIPTDATE].asByteArray();
            });
            auto filteredLineItemTable = lineItemFilter.filter(*lineitemTable);

            HashFilterJoin existJoin(Orders::ORDERKEY, LineItem::ORDERKEY);
            auto existOrderTable = existJoin.join(*filteredOrderTable, *filteredLineItemTable);

            function<uint64_t(DataRow &)> indexer = [](DataRow &row) {
                return row(Orders::ORDERPRIORITY).asInt();
            };

            HashDictAgg agg(vector<uint32_t>{2, 1}, {AGR(Orders::ORDERPRIORITY)}, []() {
                return vector<AggField *>{new agg::Count()};
            }, indexer, {pair<uint32_t, uint32_t>(0, Orders::ORDERPRIORITY)});

            auto agged = agg.agg(*existOrderTable);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[0].asByteArray() < (*b)[0].asByteArray();
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            auto printer = Printer::Make(PBEGIN PB(0) PI(1) PEND);
            printer->print(*sorted);
        }
    }
}