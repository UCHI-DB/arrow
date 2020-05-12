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
        using namespace parallel;

        void executeQ2_graph() {
            auto partSuppTable = ParquetTable::Open(PartSupp::path,
                                                    {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATIONKEY, Supplier::NAME,
                                                     Supplier::ADDRESS, Supplier::COMMENT, Supplier::PHONE,
                                                     Supplier::ACCTBAL});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME, Nation::REGIONKEY});
            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE, Part::SIZE, Part::MFGR});

            ExecutionGraph graph;

            auto partsupp = graph.add(new TableNode(partSuppTable), {});
            auto supplier = graph.add(new TableNode(supplierTable), {});
            auto nation = graph.add(new TableNode(nationTable), {});
            auto region = graph.add(new TableNode(regionTable), {});
            auto part = graph.add(new TableNode(partTable), {});

            function<bool(const ByteArray &)> typePred = [](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), q2::type, 5);
            };

            auto partFilter = graph.add(new ColFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                                      bind(sboost::Int32DictEq::build,
                                                                                           q2::size)),
                                                       new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                                          bind(sboost::ByteArrayDictMultiEq::build,
                                                                                               typePred))}), {part});
//            auto filteredPart = partFilter.filter(*partTable);

            auto regionFilter = graph.add(new ColFilter({new SimplePredicate(Region::NAME, [](const DataField &field) {
                return q2::region == (field.asByteArray());
            })}), {region});

            auto nrJoin = graph.add(new HashFilterJoin(Nation::REGIONKEY, Region::REGIONKEY), {nation, regionFilter});

            vector<uint32_t> hash_offset{0, 2};
            auto nationMat = graph.add(new HashMat(Nation::NATIONKEY, [&hash_offset](DataRow &datarow) {
                auto row = new MemDataRow(hash_offset);
                (*row)[0] = datarow[Nation::NAME];
                return unique_ptr<MemDataRow>(row);
            }), {nrJoin});

            auto snJoin = graph.add(new HashFilterJoin(Supplier::NATIONKEY, Nation::NATIONKEY), {supplier, nationMat});
//            auto filteredSupplier = snJoin.join(*supplierTable, *memNationTable);



            auto partMat = graph.add(new FilterMat(), {partFilter});
            auto supplierMat = graph.add(new FilterMat(), {snJoin});
//            FilterMat filterMat;
//            auto matPart = filterMat.mat(*filteredPart);
//            auto matSupplier = filterMat.mat(*filteredSupplier);

            // Sequence of these two joins
            auto pspJoin = graph.add(new HashFilterJoin(PartSupp::PARTKEY, Part::PARTKEY), {partsupp, partMat});
//            auto filteredPs = pspJoin.join(*partSuppTable, *matPart);

            auto pssJoin = graph.add(new HashFilterJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY), {pspJoin, supplierMat});
//            auto filteredPss = pssJoin.join(*filteredPs, *matSupplier);

            function<uint64_t(DataRow &)> hasher = [](DataRow &dr) { return dr[PartSupp::PARTKEY].asInt(); };

            auto psAgg = new HashAgg(lqf::colSize(3), {AGI(PartSupp::PARTKEY)}, []() {
                return vector<AggField *>{new agg::DoubleRecordingMin(PartSupp::SUPPLYCOST, PartSupp::SUPPKEY)};
            }, hasher);
            psAgg->useVertical();
            psAgg->useRecording();
            // PARTKEY SUPPKEY SUPPLYCOST
            auto psMinCost = graph.add(psAgg, {pssJoin});
//            auto psMinCostTable = psAgg.agg(*filteredPss);

            auto ps2partJoin = graph.add(
                    new HashColumnJoin(0, Part::PARTKEY, new ColumnBuilder({JL(0), JL(1), JRS(Part::MFGR)})),
                    {psMinCost, partMat});
            // 0 PARTKEY 1 SUPPKEY 2 P_MFGR
//            auto pswithPartTable = ps2partJoin.join(*psMinCostTable, *matPart);

            auto ps2suppJoin = graph.add(new HashColumnJoin(1, Supplier::SUPPKEY, new ColumnBuilder(
                    {JL(0), JR(Supplier::ACCTBAL), JR(Supplier::NATIONKEY), JRS(Supplier::NAME), JRS(Supplier::ADDRESS),
                     JRS(Supplier::PHONE), JRS(Supplier::COMMENT), JLS(2)})), {ps2partJoin, supplierMat});
            // 0 PARTKEY 1 ACCTBAL 2 NATIONKEY 3 SNAME 4 ADDRESS 5 PHONE 6 COMMENT 7 MFGR
//            auto psWithBothTable = ps2suppJoin.join(*pswithPartTable, *matSupplier);

            auto psWithNationJoin = graph.add(new HashColumnJoin(2, 0, new ColumnBuilder(
                    {JL(0), JL(1), JLS(3), JLS(4), JLS(5), JLS(6), JLS(7), JRS(0)})), {ps2suppJoin, nationMat});
            // 0 PARTKEY 1 ACCTBAL 2 SNAME 3 ADDRESS 4 PHONE 5 COMMENT 6 MFGR 7 NATIONNAME
//            auto alljoined = psWithNationJoin.join(*psWithBothTable, *memNationTable);

            // s_acctbal desc, n_name, s_name, p_partkey
            auto top = graph.add(new TopN(100, [](DataRow *a, DataRow *b) {
                return SDGE(1) || (SDE(1) && SBLE(7)) || (SDE(1) && SBE(7) && SBLE(2)) ||
                       (SDE(1) && SBE(7) && SBE(2) && SILE(0));
            }), {psWithNationJoin});
//            auto result = top.sort(*alljoined);

            graph.add(new Printer(PBEGIN PI(0) PD(1) /*PB(2)*/ PB(3) PB(4) PB(5) PB(6) PB(7) PEND), {top});
//            printer.print(*result);

            graph.execute();
        }

        void executeQ2_sequential() {
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
                return q2::region == (field.asByteArray());
            })});
            auto filteredRegion = regionFilter.filter(*regionTable);

            function<bool(const ByteArray &)> typePred = [](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), q2::type, 5);
            };

            ColFilter partFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                 bind(sboost::Int32DictEq::build, q2::size)),
                                  new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(sboost::ByteArrayDictMultiEq::build,
                                                                          typePred))});
            auto filteredPart = partFilter.filter(*partTable);

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



            FilterMat filterMat;
            auto matPart = filterMat.mat(*filteredPart);
            auto matSupplier = filterMat.mat(*filteredSupplier);

            // Sequence of these two joins
            HashFilterJoin pspJoin(PartSupp::PARTKEY, Part::PARTKEY);
            auto filteredPs = pspJoin.join(*partSuppTable, *matPart);

            HashFilterJoin pssJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            auto filteredPss = pssJoin.join(*filteredPs, *matSupplier);

            function<uint64_t(DataRow &)> hasher = [](DataRow &dr) { return dr[PartSupp::PARTKEY].asInt(); };
            HashAgg psAgg(lqf::colSize(3), {AGI(PartSupp::PARTKEY)}, []() {
                return vector<AggField *>{new agg::DoubleRecordingMin(PartSupp::SUPPLYCOST, PartSupp::SUPPKEY)};
            }, hasher);
            psAgg.useVertical();
            psAgg.useRecording();
            // PARTKEY SUPPKEY SUPPLYCOST
            auto psMinCostTable = psAgg.agg(*filteredPss);

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
/*
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
            cout << filteredSupplier->size() << endl;
*/
            function<bool(const ByteArray &)> typePred = [](const ByteArray &input) {
                return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 5), q2::type, 5);
            };

            ColFilter partFilter({new SboostPredicate<Int32Type>(Part::SIZE,
                                                                 bind(sboost::Int32DictEq::build, q2::size)),
                                  new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(sboost::ByteArrayDictMultiEq::build,
                                                                          typePred))});
            auto filteredPart = partFilter.filter(*partTable);
            filteredPart->blocks()->foreach([](const shared_ptr<Block> &block) {
                cout << block->size() << endl;
            });
/*
            FilterMat filterMat;
            auto matPart = filterMat.mat(*filteredPart);
            auto matSupplier = filterMat.mat(*filteredSupplier);

            // Sequence of these two joins
            HashFilterJoin pspJoin(PartSupp::PARTKEY, Part::PARTKEY);
            auto filteredPs = pspJoin.join(*partSuppTable, *matPart);

            HashFilterJoin pssJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            auto filteredPss = pssJoin.join(*filteredPs, *matSupplier);

            function<uint64_t(DataRow &)> hasher = [](DataRow &dr) { return dr[PartSupp::PARTKEY].asInt(); };
            HashAgg psAgg(lqf::colSize(3), {AGI(PartSupp::PARTKEY)}, []() {
                return vector<AggField *>{new agg::DoubleRecordingMin(PartSupp::SUPPLYCOST, PartSupp::SUPPKEY)};
            }, hasher);
            psAgg.useVertical();
            psAgg.useRecording();
            // PARTKEY SUPPKEY SUPPLYCOST
            auto psMinCostTable = psAgg.agg(*filteredPss);

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

            Printer printer(PBEGIN PI(0) PD(1) PB(3) PB(4) PB(5) PB(6) PB(7) PEND

        );
        printer.
        print(*result);
        */
        }
    }
}