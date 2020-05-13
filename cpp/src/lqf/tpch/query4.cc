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
        namespace q4 {
            ByteArray dateFrom("1993-07-01");
            ByteArray dateTo("1993-10-01");
        }
        using namespace sboost;
        using namespace q4;

        void executeQ4() {
            ExecutionGraph graph;

            auto orderTable = ParquetTable::Open(Orders::path,
                                                 {Orders::ORDERDATE, Orders::ORDERKEY, Orders::ORDERPRIORITY});
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::ORDERKEY, LineItem::RECEIPTDATE, LineItem::COMMITDATE});

            auto order = graph.add(new TableNode(orderTable), {});
            auto lineitem = graph.add(new TableNode(lineitemTable), {});

            auto orderFilter = graph.add(new ColFilter(
                    new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                       bind(&ByteArrayDictRangele::build, dateFrom, dateTo))),
                                         {order});

            auto lineItemFilter = graph.add(new RowFilter([](DataRow &datarow) {
                return datarow[LineItem::COMMITDATE].asByteArray() < datarow[LineItem::RECEIPTDATE].asByteArray();
            }), {lineitem});

            auto existJoin = graph.add(new HashFilterJoin(Orders::ORDERKEY, LineItem::ORDERKEY),
                                       {orderFilter, lineItemFilter});

            function<uint64_t(DataRow &)> indexer = [](DataRow &row) {
                return row(Orders::ORDERPRIORITY).asInt();
            };

            auto agg = graph.add(new HashAgg(vector<uint32_t>{1, 1}, {AGR(Orders::ORDERPRIORITY)}, []() {
                return vector<AggField *>{new agg::Count()};
            }, indexer), {existJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[0].asInt() < (*b)[0].asInt();
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            auto opdict = orderTable->LoadDictionary<ByteArrayType>(Orders::ORDERPRIORITY);
            auto opdictp = opdict.get();

            graph.add(new Printer(PBEGIN PDICT(opdictp, 0) PI(1) PEND),{sort});

            graph.execute();
        }

        void executeQ4Backup() {

            auto orderTable = ParquetTable::Open(Orders::path,
                                                 {Orders::ORDERDATE, Orders::ORDERKEY, Orders::ORDERPRIORITY});
            auto lineitemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::ORDERKEY, LineItem::RECEIPTDATE, LineItem::COMMITDATE});

            ColFilter orderFilter(
                    {new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                        bind(&ByteArrayDictRangele::build, dateFrom, dateTo))});
            auto filteredOrderTable = orderFilter.filter(*orderTable);

            RowFilter lineItemFilter([](DataRow &datarow) {
                return datarow[LineItem::COMMITDATE].asByteArray() < datarow[LineItem::RECEIPTDATE].asByteArray();
            });
            auto filteredLineItemTable = lineItemFilter.filter(*lineitemTable);

            HashFilterJoin existJoin(Orders::ORDERKEY, LineItem::ORDERKEY);
            auto existOrderTable = existJoin.join(*filteredOrderTable, *filteredLineItemTable);

            function<uint64_t(DataRow &)> indexer = [](DataRow &row) {
                return row(Orders::ORDERPRIORITY).asInt();
            };

            HashAgg agg(vector<uint32_t>{1, 1}, {AGR(Orders::ORDERPRIORITY)}, []() {
                return vector<AggField *>{new agg::Count()};
            }, indexer);

            auto agged = agg.agg(*existOrderTable);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[0].asInt() < (*b)[0].asInt();
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            auto opdict = orderTable->LoadDictionary<ByteArrayType>(Orders::ORDERPRIORITY);
            auto opdictp = opdict.get();
            Printer printer(PBEGIN PDICT(opdictp, 0) PI(1) PEND);
            printer.print(*sorted);
        }
    }
}