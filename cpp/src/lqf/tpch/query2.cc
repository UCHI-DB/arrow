//
// Created by harper on 3/3/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include <lqf/agg.h>
#include <lqf/print.h>
#include "tpchquery.h"

namespace lqf {
    namespace tpch {

        using namespace sboost;

        void executeQ2() {
            ByteArray region("EUROPE");
            int size = 15;
            const char *const type = "BRASS";

            auto partSuppTable = ParquetTable::Open(PartSupp::path,
                                                    {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::REGIONKEY});
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE});

            ColFilter regionFilter({new SimpleColPredicate(Region::NAME, [&region](const DataField &field) {
                return region == (*field.asByteArray());
            })});
            auto filteredRegion = regionFilter.filter(*regionTable);

            HashFilterJoin nrJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto filteredNation = nrJoin.join(*nationTable, *filteredRegion);

            MemMat nationMat(3, MLB MI(0, 0) MLE);
            auto memNationTable = nationMat.mat(*filteredNation);

            HashFilterJoin snJoin(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto filteredSupplier = snJoin.join(*supplierTable, *memNationTable);

//            auto printer = Printer::Make(PBEGIN PI(Supplier::SUPPKEY) PEND);
//            printer->print(*supplierTable);

            function<bool(const ByteArray &)> typePred = [=](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), type, 5);
            };

            ColFilter partFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                 bind(sboost::Int32DictEq::build, size)),
                                  new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(sboost::ByteArrayDictMultiEq::build,
                                                                          typePred))});
            auto filteredPart = partFilter.filter(*partTable);

            // Sequence of these two joins
            HashFilterJoin pspJoin(PartSupp::PARTKEY, Part::PARTKEY);
            auto filteredPs = pspJoin.join(*partSuppTable, *filteredPart);

            HashFilterJoin pssJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            filteredPs = pssJoin.join(*filteredPs, *filteredSupplier);
//
            FilterMat psMat;
            filteredPs = psMat.mat(*filteredPs);
//
            auto printer = Printer::Make(PBEGIN PI(0)
            PI(1)
            PEND);
            printer->print(*filteredPs);
//
//            HashAgg psAgg([]() {
//                return unique_ptr<HashCore>(new HashCore(
//                        2,
//                        [](DataRow &dr) { return dr[PartSupp::PARTKEY].asInt(); },
//                        [](DataRow &dr) {
//                            AggReducer *header = new AggReducer(1, {new agg::DoubleMin(
//                                    PartSupp::SUPPLYCOST)});
//                            header->header()[0] = dr[PartSupp::PARTKEY].asInt();
//                            return unique_ptr<AggReducer>(header);
//                        }));
//            });
//            auto psMinCostTable = psAgg.agg(*filteredPs);
//
//            cout << psMinCostTable->size() << endl;

//            Java Version from Here
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


        void executeQ2Debug() {
            char buffer[100];
            auto regionTable = ParquetTable::Open(Region::path, 3);
            regionTable->blocks()->foreach([&buffer](const shared_ptr<Block> &block) {
                auto col = (*block).col(1);
                for (int i = 0; i < 5; ++i) {
                    auto ba = (*col)[i].asByteArray();
                    memcpy((void *) buffer, ba->ptr, ba->len);
                    buffer[ba->len] = 0;
                    cout << buffer << endl;
                }
            });
        }
    }
}

//
int main(int argc, char **argv) {
    lqf::tpch::executeQ2();
}