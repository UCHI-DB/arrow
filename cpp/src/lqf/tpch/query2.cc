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
        namespace q2 {
            ByteArray region("EUROPE");
            int size = 15;
            const char *const type = "BRASS";
        }
        using namespace sboost;
        using namespace q2;

        void executeQ2() {
            auto partSuppTable = ParquetTable::Open(PartSupp::path,
                                                    {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATIONKEY, Supplier::NAME,
                                                     Supplier::ADDRESS, Supplier::COMMENT, Supplier::PHONE,
                                                     Supplier::ACCTBAL});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME, Nation::REGIONKEY});
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE, Part::MFGR});

            ColFilter regionFilter({new SimplePredicate(Region::NAME, [](const DataField &field) {
                return region == (field.asByteArray());
            })});
            auto filteredRegion = regionFilter.filter(*regionTable);

            HashFilterJoin nrJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto filteredNation = nrJoin.join(*nationTable, *filteredRegion);

            vector<uint32_t> hash_offset{0, 2};
            HashMat nationMat(Nation::NATIONKEY, [&hash_offset](DataRow &datarow) {
                auto row = new MemDataRow(hash_offset);
                (*row)[0] = datarow[Nation::NAME];
                return unique_ptr<MemDataRow>(row);
            });
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
            // PARTKEY SUPPKEY SUPPLYCOST
            auto psMinCostTable = psAgg.agg(*filteredPs);

            HashColumnJoin ps2partJoin(0, Part::PARTKEY, new ColumnBuilder({JL(0), JL(1), JRS(Part::MFGR)}));
            // 0 PARTKEY 1 SUPPKEY 2 P_MFGR
            auto pswithPartTable = ps2partJoin.join(*psMinCostTable, *matPart);

            HashColumnJoin ps2suppJoin(1, Supplier::SUPPKEY, new ColumnBuilder(
                    {JL(0), JR(Supplier::ACCTBAL), JR(Supplier::NATIONKEY), JRS(Supplier::NAME), JRS(Supplier::ADDRESS),
                     JRS(Supplier::PHONE), JRS(Supplier::COMMENT), JLS(2)}));
            // 0 PARTKEY 1 ACCTBAL 2 NATIONKEY 3 SNAME 4 ADDRESS 5 PHONE 6 COMMENT 7 MFGR
            auto psWithBothTable = ps2suppJoin.join(*pswithPartTable, *matSupplier);

            HashColumnJoin psWithNationJoin(2, 0, new ColumnBuilder(
                    {JL(0), JL(1), JLS(3), JLS(4), JLS(5), JLS(6), JLS(7), JRS(0)}));
            // 0 PARTKEY 1 ACCTBAL 2 SNAME 3 ADDRESS 4 PHONE 5 COMMENT 6 MFGR 7 NATIONNAME
            auto alljoined = psWithNationJoin.join(*psWithBothTable, *memNationTable);

            // s_acctbal desc, n_name, s_name, p_partkey
            TopN top(100, [](DataRow *a, DataRow *b) {
                return SDGE(1) || (SDE(1) && SBLE(7)) || (SDE(1) && SBE(7) && SBLE(2)) ||
                       (SDE(1) && SBE(7) && SBE(2) && SILE(0));
            });;
            auto result = top.sort(*alljoined);

            Printer printer(PBEGIN PI(0) PD(1) /*PB(2)*/ PB(3) PB(4) PB(5) PB(6) PB(7) PEND);
            printer.print(*result);
        }


        void executeQ2Debug() {
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::ADDRESS});
            supplier->blocks()->foreach([](const shared_ptr<Block> &block) {
                auto rows = block->rows();

                for (uint32_t i = 0; i < block->size(); ++i)
                    cout << i << "," << (*rows)[i][Supplier::ADDRESS].asByteArray() << endl;
            });
        }
    }
}
