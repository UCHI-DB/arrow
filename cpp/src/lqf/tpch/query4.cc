//
// Created by harper on 4/2/20.
//

#include <lqf/filter.h>
#include <lqf/data_model.h>
#include <parquet/types.h>
#include <lqf/mat.h>
#include <lqf/join.h>
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


//            Agg agg = new TableAgg(new TableReducerSource() {
//                @Override
//                public int applyAsInt(DataRow dataRow) {
//                    return dataRow.getRaw(Orders.ORDERPRIORITY);
//                }
//
//                @Override
//                public int tableSize() {
//                    return 5;
//                }
//
//                @Override
//                public RowReducer apply(DataRow dataRow) {
//                    FullDataRow header = new FullDataRow(new int[]{1, 0, 1});
//                    header.setBinary(1, dataRow.getBinary(Orders.ORDERPRIORITY));
//                    FieldsReducer fr = new FieldsReducer(header, new Count(0));
//                    return fr;
//                }
//            });
//            Table agged = agg.agg(existOrderTable);
//
//            MemSort sort = new MemSort(Comparator.comparing(row -> row.getBinary(1)));
//            Table sorted = sort.sort(agged);
//
//            new Printer.DefaultPrinter(row -> {
//                System.out.println(row.getBinary(1).toStringUsingUTF8() + "," + row.getInt(0));
//            }).print(sorted);

        }
    }
}