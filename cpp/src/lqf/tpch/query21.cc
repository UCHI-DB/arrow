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

        ByteArray status("F");

        void executeQ21() {
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERSTATUS});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SUPPKEY, LineItem::ORDERKEY, LineItem::RECEIPTDATE,
                                                LineItem::COMMITDATE});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});

            using namespace sboost;


            ColFilter orderFilter(new SboostPredicate<ByteArrayType>(Orders::ORDERSTATUS,
                                                                     bind(&ByteArrayDictEq::build,status)));
            auto validorder = orderFilter.filter(*order);

            RowFilter linedateFilter([](DataRow &row) {
                return row[LineItem::RECEIPTDATE].asByteArray() > row[LineItem::COMMITDATE].asByteArray();
            });
            auto validLineitem = linedateFilter.filter(*lineitem);

            HashFilterJoin lineWithOrderJoin(LineItem::ORDERKEY,Orders::ORDERKEY);
            validLineitem = lineWithOrderJoin.join(*validLineitem, *validorder);

            FilterMat mat;
            validLineitem = mat.mat(*validLineitem);


            ColFilter supplierNationFilter(new SboostPredicate<Int32Type>(Supplier::NATIONKEY,
                                                                          bind(&Int32DictEq::build, 3)));
            auto validSupplier = supplierNationFilter.filter(*supplier);

            HashFilterJoin lineitemSupplierFilter(LineItem::SUPPKEY, Supplier::SUPPKEY);
            auto validLineitem = lineitemSupplierFilter.join(*lineitem, *validSupplier);



            // TODO Which side is larger?
            HashFilterJoin orderonItemFilter(Orders::ORDERKEY, LineItem::ORDERKEY);
            validorder = orderonItemFilter.join(*validorder, *validLineitem);

            HashFilterJoin itemOnOrderFilter(LineItem::ORDERKEY, Orders::ORDERKEY);
            validLineitem = itemOnOrderFilter.join(validLineitem, validorder);

            Table memLineitem = new MemMat().mat(validLineitem);

            HashExistJoin existJoin(LineItem::ORDERKEY, 0,
                                               (left, right)->left.getInt(LineItem.SUPPKEY) != right.getInt(1));
            Table existLineitem = existJoin.join(lineitem, memLineitem);


            HashJoin notExistJoin = new HashNotExistJoin(LineItem.ORDERKEY, 0, (left, right)->
                    left.getInt(LineItem.SUPPKEY) != right.getInt(1) &&
                                                                           left.getBinary(LineItem.RECEIPTDATE).toStringUsingUTF8()
                                                                                   .compareTo(left.getBinary(
                                                                                           LineItem.COMMITDATE).toStringUsingUTF8()) >
                                                                           0
            );
            Table result = notExistJoin.join(lineitem, existLineitem);

            Agg countAgg = new KeyAgg(new KeyReducerSource() {
                @Override
                public long applyAsLong(DataRow from) {
                    return from.getInt(1);
                }

                @Override
                public RowReducer apply(DataRow dataRow) {
                    PrimitiveDataRow header = new PrimitiveDataRow(2);
                    header.setInt(0, dataRow.getInt(1));
                    return new FieldsReducer(header, new Count(1));
                }
            });
            Table counted = countAgg.agg(result);

            Reload supplierReload = new Reload();
            supplierReload.getLoadingProperties().setLoadColumns(Supplier.SUPPKEY, Supplier.NAME);
            validSupplier = supplierReload.reload(validSupplier);

            Join withSupplierNameJoin = new HashJoin(new RowBuilder() {
                @Override
                public DataRow build(int key, DataRow left, DataRow right) {
                    FullDataRow fd = new FullDataRow(new int[]{1, 0, 1});
                    fd.setBinary(1, left.getBinary(Supplier.NAME));
                    fd.setInt(0, right.getInt(1));
                    return fd;
                }
            }, Supplier.SUPPKEY, 0);
            result = withSupplierNameJoin.join(validSupplier, counted);

            TopN topn = new TopN(100, Comparator.nullsLast(Comparator.< DataRow > comparingInt(row->- row.getInt(0))
                    .thenComparing(row->row.getBinary(1).toStringUsingUTF8())));
            Table sorted = topn.sort(result);

            new Printer.DefaultPrinter(
                    row->System.out.println(row.getBinary(1).toStringUsingUTF8() + "," + row.getInt(0)))
                    .print(sorted);

            System.out.println(System.currentTimeMillis() - startTime);
        }

    }
}