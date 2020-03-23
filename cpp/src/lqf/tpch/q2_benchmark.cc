//
// Created by harper on 3/3/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include "tpch_query.h"

namespace lqf {
    namespace tpch {

        using namespace sboost;

        void executeQ2() {
            ByteArray region("EUROPE");
            int size = 15;
            const char *const type = "BRASS";

            auto partSuppTable = ParquetTable::Open(PartSupp::path, {PartSupp::PARTKEY, PartSupp::SUPPKEY});

            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});
//            supplierTable.setSnapshot(source->
//            {
//                SupplierRow res = new SupplierRow();
//                res.s_acctbal = source.getDouble(Supplier.ACCTBAL);
//                res.s_address = source.getBinary(Supplier.ADDRESS);
//                res.s_comment = source.getBinary(Supplier.COMMENT);
//                res.s_name = source.getBinary(Supplier.NAME);
//                res.s_phone = source.getBinary(Supplier.PHONE);
//                res.s_nationkey = source.getInt(Supplier.NATIONKEY);
//                return res;
//            });

            auto nationTable = ParquetTable::Open(Nation::path, {Nation::REGIONKEY});
//            nationTable.setSnapshot(source->
//            {
//                FullDataRow ndr = new FullDataRow(new int[]{1, 0, 1});
//                ndr.setInt(0, source.getInt(Nation.NATIONKEY));
//                ndr.setBinary(1, source.getBinary(Nation.NAME));
//                return ndr;
//            });

            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});

            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE});
//            partTable.setSnapshot(source->
//            {
//                PartRow pr = new PartRow();
//                pr.p_mfgr = source.getBinary(Part.MFGR);
//                return pr;
//            });

            ColFilter regionFilter({new SimpleColPredicate(Region::NAME, [&region](const DataField &field) {
                return (*field.asByteArray()) == region;
            })});
            auto filteredRegion = regionFilter.filter(*regionTable);

            HashFilterJoin nrJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto filteredNation = nrJoin.join(*nationTable, *filteredRegion);

//            Reload reloadNation = new Reload();
//            reloadNation.getLoadingProperties().setLoadColumns(new int[]{Nation.NATIONKEY, Nation.NAME});
            MemMat nationMat(3, MLB MI(0, 0)
                MB(1, 1)
                MI(2, 2) MLE);
            auto memNationTable = nationMat.mat(*filteredNation);

            HashFilterJoin snJoin(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto filteredSupplier = snJoin.join(*supplierTable, *memNationTable);

            function<bool(const ByteArray &)> typePred = [=](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), type, 5);
            };

            ColFilter partFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                 bind(sboost::Int32DictEq::build, size)),
                                  new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(sboost::ByteArrayDictMultiEq::build,
                                                                          typePred))});
            auto filteredPart = partFilter.filter(*partTable);

//            // Sequence of these two joins
//            Join pspJoin = new HashFilterJoin(PartSupp.PARTKEY, Part.PARTKEY);
//            Table filteredPs = pspJoin.join(partSuppTable, filteredPart);
//
//            Join pssJoin = new HashFilterJoin(PartSupp.SUPPKEY, Supplier.SUPPKEY);
//            filteredPs = pssJoin.join(filteredPs, filteredSupplier);
//
//            FilterMat psMat = new FilterMat();
//            filteredPs = psMat.mat(filteredPs);
//
//            Reload psReload = new Reload();
//            psReload.getLoadingProperties().setLoadColumns(new int[]{PartSupp.PARTKEY, PartSupp.SUPPLYCOST});
//            Table reloadedPs = psReload.reload(filteredPs);
//
//            Agg psAgg = new KeyAgg(new KeyReducerSource() {
//                @Override
//                public RowReducer apply(DataRow key) {
//                    PrimitiveDataRow header = new PrimitiveDataRow(2);
//                    header.setInt(0, key.getInt(PartSupp.PARTKEY));
//                    FieldsReducer fr = new FieldsReducer(header, new DoubleMin(PartSupp.SUPPLYCOST, 1));
//                    return fr;
//                }
//
//                @Override
//                public long applyAsLong(DataRow dataRow) {
//                    return dataRow.getInt(PartSupp.PARTKEY);
//                }
//            });
//            Table psMinCostTable = psAgg.agg(reloadedPs);
//
//            System.out.println(psMinCostTable.size());
//
//            partSuppTable.getLoadingProperties().setLoadColumns(
//                    new int[]{PartSupp.PARTKEY, PartSupp.SUPPKEY, PartSupp.SUPPLYCOST});
//            Join psmcJoin = new HashJoin(new RowBuilder() {
//                @Override
//                public DataRow build(int key, DataRow left, DataRow right) {
//                    PrimitiveDataRow result = new PrimitiveDataRow(3);
//                    result.setInt(0, key);
//                    result.setInt(1, left.getInt(PartSupp.SUPPKEY));
//                    return result;
//                }
//            }, PartSupp.PARTKEY, 0,
//                    (left, right)->left.getDouble(PartSupp.SUPPLYCOST) == right.getDouble(1));
//            Table pswithMinCost = psmcJoin.join(filteredPs, psMinCostTable);
//
//            partTable.getLoadingProperties().setLoadColumns(new int[]{Part.PARTKEY, Part.MFGR});
//            Join ps2part = new HashJoin(new RowBuilder() {
//                @Override
//                public DataRow build(int key, DataRow left, DataRow right) {
//                    FullDataRow res = new FullDataRow(new int[]{3, 1, 6});
//                    PartRow pr = (PartRow) right;
//                    res.setBinary(4, pr.p_mfgr);
//                    res.setInt(0, key);
//                    res.setInt(2, left.getInt(1));// SUPPKEY
//                    return res;
//                }
//            }, 0, Part.PARTKEY);
//            Table ps2partTable = ps2part.join(pswithMinCost, filteredPart);
//
//
//            // TODO When supplier is large, this can be replaced with BlockHashJoin
//            supplierTable.getLoadingProperties().setLoadColumns(new int[]{
//                    Supplier.SUPPKEY, Supplier.ACCTBAL,
//                    Supplier.NATIONKEY, Supplier.COMMENT,
//                    Supplier.PHONE, Supplier.NAME,
//                    Supplier.ADDRESS, Supplier.NATIONKEY
//            });
//            Join ps2supp = new HashJoin(new RowBuilder() {
//                @Override
//                public DataRow build(int key, DataRow left, DataRow right) {
//                    SupplierRow sr = (SupplierRow) right;
//                    left.setDouble(3, sr.s_acctbal);
//                    left.setBinary(7, sr.s_address);
//                    left.setBinary(9, sr.s_comment);
//                    left.setBinary(5, sr.s_name);
//                    left.setBinary(8, sr.s_phone);
//                    left.setInt(1, sr.s_nationkey);
//                    return left;
//                }
//            }, 2, Supplier.SUPPKEY);
//            Table result = ps2supp.join(ps2partTable, filteredSupplier);
//
//            Join supp2nationJoin = new HashJoin(new RowBuilder() {
//                @Override
//                public DataRow build(int key, DataRow left, DataRow right) {
//                    left.setBinary(6, right.getBinary(1));
//                    return left;
//                }
//            }, 1, Nation.NATIONKEY);
//            result = supp2nationJoin.join(result, memNationTable);
//
//
//            TopN sort = new TopN(100,
//                                 Comparator.nullsLast(Comparator.< DataRow > comparingDouble(row->- row.getDouble(3))
//                                         .thenComparing(row->row.getBinary(6).toStringUsingUTF8())
//                                         .thenComparing(row->row.getBinary(5).toStringUsingUTF8())
//                                         .thenComparingInt(row->row.getInt(0))));
//            result = sort.sort(result);
//
//            Printer.DefaultPrinter
//            printer = new Printer.DefaultPrinter();
//            printer.print(result);

        }
    }
}


int main(int argc, char **argv) {

}