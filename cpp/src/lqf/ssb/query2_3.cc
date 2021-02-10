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
            auto partTable = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::BRAND});
            auto lineorderTable = ParquetTable::Open(LineOrder::path, {LineOrder::PARTKEY, LineOrder::SUPPKEY,
                                                                       LineOrder::ORDERDATE, LineOrder::REVENUE});

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

        void executeQ2_3Graph() {
            ExecutionGraph graph;

            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::REGION});
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::CATEGORY, Part::BRAND});
            auto lineorder = ParquetTable::Open(LineOrder::path, {LineOrder::PARTKEY, LineOrder::SUPPKEY,
                                                                  LineOrder::ORDERDATE, LineOrder::REVENUE});

            auto supplierTable = graph.add(new TableNode(supplier), {});
            auto partTable = graph.add(new TableNode(part), {});
            auto lineorderTable = graph.add(new TableNode(lineorder), {});

            auto partFilter = graph.add(new ColFilter(new SboostPredicate<ByteArrayType>(
                    Part::BRAND, bind(ByteArrayDictEq::build, brand))), {partTable});

            auto suppFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region))),
                    {supplierTable});

            auto suppFilterJoin = graph.add(new FilterJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY),
                                            {lineorderTable, suppFilter});

            auto partJoin = graph.add(new HashJoin(LineOrder::PARTKEY, Part::PARTKEY, new Q2RowBuilder()),
                                      {suppFilterJoin, partFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data(1).asInt() << 16) + data(2).asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(0)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 1, 0)
                                                     ->field(F_REGULAR, 2, 1)->buildSnapshot(),
                                             aggFields), {partJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PD(2) PI(0) PI(1) PEND), {sort});

            graph.execute(true);
        }

        void executeQ2_3() {
            ExecutionGraph graph;

            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::REGION});
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::CATEGORY, Part::BRAND});
            auto lineorder = ParquetTable::Open(LineOrder::path, {LineOrder::PARTKEY, LineOrder::SUPPKEY,
                                                                  LineOrder::ORDERDATE, LineOrder::REVENUE});

            auto supplierTable = graph.add(new TableNode(supplier), {});
            auto partTable = graph.add(new TableNode(part), {});
            auto lineorderTable = graph.add(new TableNode(lineorder), {});

            auto partFilter = graph.add(new ColFilter(new SboostPredicate<ByteArrayType>(
                    Part::BRAND, bind(ByteArrayDictEq::build, brand))), {partTable});

            auto suppFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region))),
                    {supplierTable});

            auto suppFilterJoin = graph.add(new FilterJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY),
                                            {lineorderTable, suppFilter});

            auto partJoin = graph.add(new encopr::EncHashJoin(LineOrder::PARTKEY, Part::PARTKEY, new Q2RowBuilder(),
                                                              {parquet::Type::type::DOUBLE, parquet::Type::type::INT32,
                                                               parquet::Type::type::INT32},
                                                              {encoding::EncodingType::PLAIN, encoding::EncodingType::DICTIONARY,
                                                               encoding::EncodingType::DICTIONARY}),
                                      {suppFilterJoin, partFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data(1).asInt() << 16) + data(2).asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(0)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 1, 0)
                                                     ->field(F_REGULAR, 2, 1)->buildSnapshot(),
                                             aggFields), {partJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PD(2) PI(0) PI(1) PEND), {sort});

            graph.execute(true);
        }
    }
}
