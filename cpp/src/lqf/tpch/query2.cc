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
#include <lqf/sort.h>
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

            ColFilter regionFilter({new SimplePredicate(Region::NAME, [&region](const DataField &field) {
                return region == (field.asByteArray());
            })});
            auto filteredRegion = regionFilter.filter(*regionTable);

            HashFilterJoin nrJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto filteredNation = nrJoin.join(*nationTable, *filteredRegion);

            MemMat nationMat(3, MLB MI(0, 0)
                MB(2, 1) MLE);
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


            FilterMat filterMat;
            auto matPart = filterMat.mat(*filteredPart);
            auto matSupplier = filterMat.mat(*filteredSupplier);

            // Sequence of these two joins
            HashFilterJoin pspJoin(PartSupp::PARTKEY, Part::PARTKEY);
            auto filteredPs = pspJoin.join(*partSuppTable, *matPart);


            HashFilterJoin pssJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            filteredPs = pssJoin.join(*filteredPs, *matSupplier);
//
            FilterMat psMat;
            filteredPs = psMat.mat(*filteredPs);
//
            function<uint64_t(DataRow &)> hasher = [](DataRow &dr) { return dr[PartSupp::PARTKEY].asInt(); };
            HashAgg psAgg(lqf::colSize(3), {AGI(PartSupp::PARTKEY)}, []() {
                return vector<AggField *>{new agg::DoubleRecordingMin(PartSupp::SUPPLYCOST, PartSupp::SUPPKEY)};
            }, hasher);
            psAgg.useVertical();
            psAgg.useRecording();

            // 0: PARTKEY 1: SUPPLYCOST 2: SUPPKEY
            auto psMinCostTable = psAgg.agg(*filteredPs);

            HashColumnJoin ps2partJoin(0, Part::PARTKEY, new ColumnBuilder({JL(0), JL(2), JRS(Part::MFGR)}));
            // 0 PARTKEY 1 SUPPKEY 2 P_MFGR
            auto pswithPartTable = ps2partJoin.join(*psMinCostTable, *matPart);

            HashColumnJoin ps2suppJoin(1, Supplier::SUPPKEY, new ColumnBuilder(
                    {JL(0), JR(Supplier::ACCTBAL), JR(Supplier::NATIONKEY), JRS(Supplier::NAME), JRS(Supplier::ADDRESS),
                     JRS(Supplier::PHONE), JRS(Supplier::COMMENT), JLS(2)}));
            // 0 PARTKEY 1 ACCTBAL 2 NATIONKEY 3 SNAME 4 ADDRESS 5 PHONE 6 COMMENT 7 MFGR
            auto psWithBothTable = ps2suppJoin.join(*pswithPartTable, *matSupplier);

            HashColumnJoin psWithNationJoin(2, 0, new ColumnBuilder(
                    {JL(0), JL(1), JLS(3), JLS(4), JLS(5), JLS(6), JLS(7), JRS(1)}));
            // 0 PARTKEY 1 ACCTBAL 2 SNAME 3 ADDRESS 4 PHONE 5 COMMENT 6 MFGR 7 NATIONNAME
            auto alljoined = psWithNationJoin.join(*psWithBothTable, *memNationTable);

            // s_acctbal desc, n_name, s_name, p_partkey
            TopN top(100, [](DataRow *a, DataRow *b) {
                return SDGE(1) || (SDE(1) && SBLE(7)) || (SDE(1) && SBE(7) && SBLE(2)) ||
                       (SDE(1) && SBE(7) && SBE(2) && SILE(0));
            });
//            TopN sort = new TopN(100,
//                                 Comparator.nullsLast(Comparator.< DataRow > comparingDouble(row->- row.getDouble(3))
//                                         .thenComparing(row->row.getBinary(6).toStringUsingUTF8())
//                                         .thenComparing(row->row.getBinary(5).toStringUsingUTF8())
//                                         .thenComparingInt(row->row.getInt(0))));
            auto result = top.sort(*alljoined);

            auto printer = Printer::Make(PBEGIN PI(0) PD(1) PB(2) PB(3) PB(4) PB(5) PB(6) PB(7) PB(8) PEND);
            printer->
                    print(*result);
        }


        void executeQ2Debug() {
            char buffer[100];
            auto regionTable = ParquetTable::Open(Region::path, 3);
            regionTable->blocks()->foreach([&buffer](const shared_ptr<Block> &block) {
                auto col = (*block).col(1);
                for (int i = 0; i < 5; ++i) {
                    auto ba = (*col)[i].asByteArray();
                    memcpy((void *) buffer, ba.ptr, ba.len);
                    buffer[ba.len] = 0;
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