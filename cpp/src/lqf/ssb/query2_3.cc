//
// Created by Harper on 1/2/21.
//

#include "query2.h"

namespace lqf {
    namespace ssb {

        namespace q2_3 {
            ByteArray brand("MFGR#2221");
            ByteArray region("EUROPE");
        }

        using namespace q2;
        using namespace q2_3;
        using namespace sboost;

        void executeQ2_3Plain() {
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::REGION});
            auto partTable = ParquetTable::Open(Supplier::path, {Part::PARTKEY, Part::BRAND});
            auto lineorderTable = ParquetTable::Open(Supplier::path,
                                                     {LineOrder::PARTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE});

            ColFilter partFilter(new SboostPredicate<ByteArrayType>(
                    Part::BRAND, bind(ByteArrayDictEq::build, brand)));
            auto filteredPart = partFilter.filter(*partTable);

            ColFilter suppFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupplier = suppFilter.filter(*supplierTable);

            FilterJoin suppFilterJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY);
            auto filteredLineorder = suppFilterJoin.join(*lineorderTable, *filteredSupplier);

            HashJoin partJoin(LineOrder::PARTKEY, Part::PARTKEY, new Q2RowBuilder());
            auto withPart = partJoin.join(*filteredLineorder, *filteredPart);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data(1).asInt() << 16) + data(2).asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(0)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 1, 0)
                                ->field(F_REGULAR, 2, 1)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*withPart);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PD(2) PI(0) PI(1) PEND);
            printer.print(*sorted);;
        }

        void executeQ2_1() {

        }
    }
}
