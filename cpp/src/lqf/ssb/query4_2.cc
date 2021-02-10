//
// Created by Harper on 1/3/21.
//

#include "query4.h"

namespace lqf {
    namespace ssb {
        namespace q4_2 {
            class OrderProfitBuilder : public RowBuilder {
            public:
                OrderProfitBuilder() : RowBuilder(
                        {JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE), JL(LineOrder::SUPPLYCOST),
                         JL(LineOrder::PARTKEY), JRR(Supplier::NATION)}, false, false) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = udf::date2year(left[LineOrder::ORDERDATE].asByteArray());
                    target[1] = right(0).asInt();
                    target[2] = left[LineOrder::PARTKEY].asInt();
                    target[3] = left[LineOrder::REVENUE].asDouble() - left[LineOrder::SUPPLYCOST].asDouble();
                }
            };

            class OrderProfitColBuilder : public ColumnBuilder {
            public:
                OrderProfitColBuilder() : ColumnBuilder(
                        {JL(LineOrder::ORDERDATE), JRR(Supplier::NATION), JL(LineOrder::PARTKEY),
                         JL(LineOrder::REVENUE)}) {}

                shared_ptr<MemvBlock> cacheToMem(Block &input) override {
                    auto block_size = input.size();
                    auto memcache = make_shared<MemvBlock>(block_size, load_col_size_);

                    // Copy year
                    {
                        auto reader = input.col(LineOrder::ORDERDATE);
                        auto writer = memcache->col(0);
                        for (uint32_t j = 0; j < block_size; ++j) {
                            (*writer)[j] = udf::date2year(reader->next().asByteArray());
                        }
                        writer->close();
                    }
                    {
                        auto reader = input.col(LineOrder::PARTKEY);
                        auto writer = memcache->col(1);
                        for (uint32_t j = 0; j < block_size; ++j) {
                            (*writer)[j] = reader->next().asInt();
                        }
                        writer->close();
                    }
                    // Get PROFIT
                    {
                        auto revreader = input.col(LineOrder::REVENUE);
                        auto screader = input.col(LineOrder::SUPPLYCOST);
                        auto profitwriter = memcache->col(2);
                        for (uint32_t j = 0; j < block_size; ++j) {
                            (*profitwriter)[j] = revreader->next().asDouble() - screader->next().asDouble();
                        }
                        profitwriter->close();
                    }

                    return memcache;
                }
            };

            ByteArray region("AMERICA");
            ByteArray mfgr1("MFGR#1");
            ByteArray mfgr2("MFGR#2");
            ByteArray year_from("19970101");
            ByteArray year_to("19981231");
        }

        using namespace q4;
        using namespace q4_2;
        using namespace sboost;

        void executeQ4_2Plain() {
            auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::REGION});
            auto partTable = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY, Part::CATEGORY});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::REGION, Supplier::NATION});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::ORDERDATE, LineOrder::SUPPKEY, LineOrder::PARTKEY,
                                                      LineOrder::CUSTKEY, LineOrder::REVENUE, LineOrder::SUPPLYCOST});

            ColFilter suppFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupp = suppFilter.filter(*supplierTable);

            ColFilter custFilter(new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            function<bool(const ByteArray &)> pred = [=](const ByteArray &input) {
                return input == mfgr1 || input == mfgr2;
            };
            ColFilter partFilter(new SBoostByteArrayPredicate(Part::MFGR, bind(ByteArrayDictMultiEq::build, pred)));
            auto filteredPart = partFilter.filter(*partTable);

            ColFilter orderFilter(new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                               bind(ByteArrayDictBetween::build, year_from, year_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            FilterJoin custFilterJoin(LineOrder::CUSTKEY, Customer::CUSTKEY);
            auto orderOnValidCust = custFilterJoin.join(*filteredOrder, *filteredCustomer);

            HashJoin withSuppJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY, new OrderProfitBuilder());
            auto orderWithSupp = withSuppJoin.join(*orderOnValidCust, *filteredSupp);

            HashJoin withPartJoin(2, Part::PARTKEY,
                                  new RowBuilder({JL(0), JL(1), JRR(Part::CATEGORY), JL(3)}, false, false));
            auto validOrder = withPartJoin.join(*orderWithSupp, *filteredPart);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 20) + (data[1].asInt() << 10) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*validOrder);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1)) || (SIE(0) && SIE(1) && SILE(2));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);;
        }

        void executeQ4_2Column() {
            auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::REGION});
            auto partTable = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY, Part::CATEGORY});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::REGION, Supplier::NATION});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::ORDERDATE, LineOrder::SUPPKEY, LineOrder::PARTKEY,
                                                      LineOrder::CUSTKEY, LineOrder::REVENUE, LineOrder::SUPPLYCOST});

            ColFilter suppFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupp = suppFilter.filter(*supplierTable);

            ColFilter custFilter(new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            function<bool(const ByteArray &)> pred = [=](const ByteArray &input) {
                return input == mfgr1 || input == mfgr2;
            };
            ColFilter partFilter(new SBoostByteArrayPredicate(Part::MFGR, bind(ByteArrayDictMultiEq::build, pred)));
            auto filteredPart = partFilter.filter(*partTable);

            ColFilter orderFilter(new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                               bind(ByteArrayDictBetween::build, year_from, year_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            FilterJoin custFilterJoin(LineOrder::CUSTKEY, Customer::CUSTKEY);
            auto orderOnValidCust = custFilterJoin.join(*filteredOrder, *filteredCustomer);

            ParquetHashColumnJoin withSuppJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY, new OrderProfitColBuilder());
            auto orderWithSupp = withSuppJoin.join(*orderOnValidCust, *filteredSupp);

            HashColumnJoin withPartJoin(2, Part::PARTKEY,
                                        new ColumnBuilder({JL(0), JL(1), JRR(Part::CATEGORY), JL(3)}), true);
            auto validOrder = withPartJoin.join(*orderWithSupp, *filteredPart);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 20) + (data[1].asInt() << 10) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*validOrder);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1)) || (SIE(0) && SIE(1) && SILE(2));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);;
        }

        void executeQ4_2Graph() {
            ExecutionGraph graph;

            auto customer = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::REGION});
            auto part = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY, Part::CATEGORY});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});
            auto lineorder = ParquetTable::Open(LineOrder::path,
                                                {LineOrder::ORDERDATE, LineOrder::SUPPKEY, LineOrder::PARTKEY,
                                                 LineOrder::CUSTKEY, LineOrder::REVENUE, LineOrder::SUPPLYCOST});

            auto customerTable = graph.add(new TableNode(customer), {});
            auto partTable = graph.add(new TableNode(part), {});
            auto supplierTable = graph.add(new TableNode(supplier), {});
            auto lineorderTable = graph.add(new TableNode(lineorder), {});

            auto suppFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region))),
                    {supplierTable});

            auto custFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region))),
                    {customerTable});

            function<bool(const ByteArray &)> pred = [=](const ByteArray &input) {
                return input == mfgr1 || input == mfgr2;
            };
            auto partFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Part::MFGR, bind(ByteArrayDictMultiEq::build, pred))),
                    {partTable});

            auto orderFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, year_from, year_to))),
                                         {lineorderTable});


            auto custFilterJoin = graph.add(new FilterJoin(LineOrder::CUSTKEY, Customer::CUSTKEY),
                                            {orderFilter, custFilter});

            auto withSuppJoin = graph.add(
                    new ParquetHashColumnTJoin<Hash32SparseContainer>(LineOrder::SUPPKEY, Supplier::SUPPKEY,
                                                                      new OrderProfitColBuilder()),
                    {custFilterJoin, suppFilter});
            // YEAR, NATION, PARTKEY, REV

            auto withPartJoin = graph.add(new HashColumnTJoin<Hash32SparseContainer>(
                    2, Part::PARTKEY, new ColumnBuilder({JL(0), JL(1), JRR(Part::CATEGORY), JL(3)}), true),
                                          {withSuppJoin, partFilter});
            // YEAR, NATION, CATEGORY, REV


            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 20) + (data[1].asInt() << 10) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields), {withPartJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1)) || (SIE(0) && SIE(1) && SILE(2));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND), {sort});

            graph.execute(true);
        }

        void executeQ4_2() {
            ExecutionGraph graph;

            auto customer = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::REGION});
            auto part = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY, Part::CATEGORY});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});
            auto lineorder = ParquetTable::Open(LineOrder::path,
                                                {LineOrder::ORDERDATE, LineOrder::SUPPKEY, LineOrder::PARTKEY,
                                                 LineOrder::CUSTKEY, LineOrder::REVENUE, LineOrder::SUPPLYCOST});

            auto customerTable = graph.add(new TableNode(customer), {});
            auto partTable = graph.add(new TableNode(part), {});
            auto supplierTable = graph.add(new TableNode(supplier), {});
            auto lineorderTable = graph.add(new TableNode(lineorder), {});

            auto suppFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region))),
                    {supplierTable});

            auto custFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region))),
                    {customerTable});

            function<bool(const ByteArray &)> pred = [=](const ByteArray &input) {
                return input == mfgr1 || input == mfgr2;
            };
            auto partFilter = graph.add(
                    new ColFilter(new SBoostByteArrayPredicate(Part::MFGR, bind(ByteArrayDictMultiEq::build, pred))),
                    {partTable});

            auto orderFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, year_from, year_to))),
                                         {lineorderTable});


            auto custFilterJoin = graph.add(new FilterJoin(LineOrder::CUSTKEY, Customer::CUSTKEY),
                                            {orderFilter, custFilter});

            auto withSuppJoin = graph.add(new encopr::EncHashTJoin<Hash32SparseContainer>(
                    LineOrder::SUPPKEY, Supplier::SUPPKEY,
                    new OrderProfitBuilder(),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::INT32,
                     parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::DICTIONARY, encoding::EncodingType::DICTIONARY,
                     encoding::EncodingType::BITPACK,
                     encoding::EncodingType::PLAIN}),
                                          {custFilterJoin, suppFilter});
            // YEAR, NATION, PARTKEY, REV

            auto withPartJoin = graph.add(new encopr::EncHashTJoin<Hash32SparseContainer>(
                    2, Part::PARTKEY, new RowBuilder({JL(0), JL(1), JRR(Part::CATEGORY), JL(3)}, false, true),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::INT32,
                     parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::DICTIONARY, encoding::EncodingType::DICTIONARY,
                     encoding::EncodingType::DICTIONARY,
                     encoding::EncodingType::PLAIN}),
                                          {withSuppJoin, partFilter});
            // YEAR, NATION, CATEGORY, REV


            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 20) + (data[1].asInt() << 10) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields), {withPartJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SILE(1)) || (SIE(0) && SIE(1) && SILE(2));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND), {sort});

            graph.execute(true);
        }
    }
}
