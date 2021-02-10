//
// Created by Harper on 1/3/21.
//

#include "query3.h"

namespace lqf {
    namespace ssb {
        namespace q3_1 {
            ByteArray region("ASIA");
            ByteArray date_from("19920101");
            ByteArray date_to("19971231");

            class WithNationBuilder : public RowBuilder {
            public:
                WithNationBuilder() : RowBuilder(
                        {JL(LineOrder::CUSTKEY), JRR(Supplier::NATION),
                         JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = left[LineOrder::CUSTKEY].asInt();
                    target[1] = right[0].asInt();
                    target[2] = udf::date2year(left[LineOrder::ORDERDATE].asByteArray());
                    target[3] = left[LineOrder::REVENUE].asDouble();
                }
            };

            class WithNationColBuilder : public ColumnBuilder {
            public:
                WithNationColBuilder() : ColumnBuilder({JL(LineOrder::CUSTKEY), JRR(Supplier::NATION),
                                                        JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE)}) {}

                shared_ptr<MemvBlock> cacheToMem(Block &input) override {
                    auto block_size = input.size();
                    auto memcache = make_shared<MemvBlock>(block_size, load_col_size_);

                    copyColumn(block_size, *memcache, 0, input, LineOrder::CUSTKEY);

                    // Copy year
                    auto reader = input.col(LineOrder::ORDERDATE);
                    auto writer = memcache->col(1);
                    for (uint32_t j = 0; j < block_size; ++j) {
                        (*writer)[j] = udf::date2year(reader->next().asByteArray());
                    }

                    copyColumn(block_size, *memcache, 2, input, LineOrder::REVENUE);

                    return memcache;
                }
            };
        }

        using namespace q3;
        using namespace q3_1;
        using namespace sboost;


        void executeQ3_1Plain() {
            auto customerTable = ParquetTable::Open(Customer::path,
                                                    {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                      LineOrder::REVENUE});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            ColFilter supplierFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupplier = supplierFilter.filter(*supplierTable);

            ColFilter custFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            ColFilter orderFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            HashTJoin<Hash32SparseContainer> orderSupplierJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY,
                                                               new WithNationBuilder(), nullptr, 10000);
            // CUSTKEY, S_NATION, YEAR, REVENUE
            auto orderWithSupp = orderSupplierJoin.join(*filteredOrder, *filteredSupplier);

            HashColumnTJoin<Hash32SparseContainer> allJoin(0, Customer::CUSTKEY,
                                                           new ColumnBuilder(
                                                                   {JRR(Customer::NATION), JL(1), JL(2), JL(3)}), true);
            auto allJoined = allJoin.join(*orderWithSupp, *filteredCustomer);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*allJoined);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);
        }

        void executeQ3_1PlainEnc() {
            auto customerTable = ParquetTable::Open(Customer::path,
                                                    {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                      LineOrder::REVENUE});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            ColFilter supplierFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupplier = supplierFilter.filter(*supplierTable);

            ColFilter custFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            ColFilter orderFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            encopr::EncHashTJoin<Hash32SparseContainer> orderSupplierJoin(
                    LineOrder::SUPPKEY, Supplier::SUPPKEY, new WithNationBuilder(),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::INT32,
                     parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::BITPACK, encoding::EncodingType::PLAIN, encoding::EncodingType::PLAIN,
                     encoding::EncodingType::PLAIN}, nullptr,
                    10000);
            // CUSTKEY, S_NATION, YEAR, REVENUE
            auto orderWithSupp = orderSupplierJoin.join(*filteredOrder, *filteredSupplier);

            encopr::EncHashTJoin<Hash32SparseContainer> allJoin(
                    0, Customer::CUSTKEY, new RowBuilder({JRR(Customer::NATION), JL(1), JL(2), JL(3)}, false, true),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::INT32,
                     parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::BITPACK, encoding::EncodingType::PLAIN,
                     encoding::EncodingType::PLAIN, encoding::EncodingType::PLAIN});
            auto allJoined = allJoin.join(*orderWithSupp, *filteredCustomer);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*allJoined);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);
        }

        void executeQ3_1Column() {
            auto customerTable = ParquetTable::Open(Customer::path,
                                                    {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                      LineOrder::REVENUE});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            ColFilter supplierFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupplier = supplierFilter.filter(*supplierTable);

            ColFilter custFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            ColFilter orderFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            ParquetHashColumnTJoin<Hash32SparseContainer> orderSupplierJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY,
                                                                            new WithNationColBuilder());
            // CUSTKEY, S_NATION, YEAR, REVENUE
            auto orderWithSupp = orderSupplierJoin.join(*filteredOrder, *filteredSupplier);

            HashColumnTJoin<Hash32SparseContainer> allJoin(0, Customer::CUSTKEY,
                                                           new ColumnBuilder(
                                                                   {JRR(Customer::NATION), JL(1), JL(2), JL(3)}), true);
            auto allJoined = allJoin.join(*orderWithSupp, *filteredCustomer);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*allJoined);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);
        }

        void executeQ3_1() {
            ExecutionGraph graph;

            auto customer = ParquetTable::Open(Customer::path,
                                               {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorder = ParquetTable::Open(LineOrder::path,
                                                {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                 LineOrder::REVENUE});
            auto supplier = ParquetTable::Open(Supplier::path,
                                               {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            auto customerTable = graph.add(new TableNode(customer), {});
            auto lineorderTable = graph.add(new TableNode(lineorder), {});
            auto supplierTable = graph.add(new TableNode(supplier), {});

            auto supplierFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region))),
                                            {supplierTable});
            auto custFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region))),
                                        {customerTable});
            auto orderFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to))),
                                         {lineorderTable});

//            auto orderSupplierJoin = graph.add(
//                    new HashTJoin<Hash32SparseContainer>(LineOrder::SUPPKEY, Supplier::SUPPKEY, new WithNationBuilder()),
//                    {orderFilter, supplierFilter});
//             This has large mem size
            auto orderSupplierJoin = graph.add(
                    new ParquetHashColumnTJoin<Hash32SparseContainer>(LineOrder::SUPPKEY, Supplier::SUPPKEY,
                                                                      new WithNationColBuilder()),
                    {orderFilter, supplierFilter});

            // CUSTKEY, S_NATION, YEAR, REVENUE

            auto allJoin = graph.add(new HashColumnTJoin<Hash32SparseContainer>(0, Customer::CUSTKEY, new ColumnBuilder(
                    {JRR(Customer::NATION), JL(1), JL(2), JL(3)}), true),
                                     {orderSupplierJoin, custFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields), {allJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND), {sort});

            graph.execute(true);
        }

        void executeQ3_1Enc() {
            ExecutionGraph graph;

            auto customer = ParquetTable::Open(Customer::path,
                                               {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorder = ParquetTable::Open(LineOrder::path,
                                                {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                 LineOrder::REVENUE});
            auto supplier = ParquetTable::Open(Supplier::path,
                                               {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            auto customerTable = graph.add(new TableNode(customer), {});
            auto lineorderTable = graph.add(new TableNode(lineorder), {});
            auto supplierTable = graph.add(new TableNode(supplier), {});

            auto supplierFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region))),
                                            {supplierTable});
            auto custFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region))),
                                        {customerTable});
            auto orderFilter = graph.add(new ColFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to))),
                                         {lineorderTable});

            auto orderSupplierJoin = graph.add(new encopr::EncHashTJoin<Hash32SparseContainer>(
                    LineOrder::SUPPKEY, Supplier::SUPPKEY,
                    new WithNationBuilder(),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::INT32,
                     parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::BITPACK, encoding::EncodingType::DICTIONARY,
                     encoding::EncodingType::DICTIONARY, encoding::EncodingType::PLAIN}),
                                               {orderFilter, supplierFilter});

            // CUSTKEY, S_NATION, YEAR, REVENUE

            auto allJoin = graph.add(new encopr::EncHashTJoin<Hash32SparseContainer>(
                    0, Customer::CUSTKEY, new RowBuilder({JRR(Customer::NATION), JL(1), JL(2), JL(3)}, false, true),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::INT32,
                     parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::DICTIONARY, encoding::EncodingType::DICTIONARY,
                     encoding::EncodingType::DICTIONARY, encoding::EncodingType::PLAIN}),
                                     {orderSupplierJoin, custFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields), {allJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND), {sort});

            graph.execute(true);
        }

        void executeQ3_1PlainDebug() {
            auto customerTable = ParquetTable::Open(Customer::path,
                                                    {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                      LineOrder::REVENUE});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            ColFilter supplierFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupplier = supplierFilter.filter(*supplierTable);

            ColFilter custFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            ColFilter orderFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            WithNationBuilder builder;
            builder.on(*filteredOrder, *filteredSupplier);
            builder.init();

            std::cout << filteredCustomer->size() << endl;
//            auto container = HashBuilder::buildContainer(*filteredSupplier, Supplier::SUPPKEY, builder.snapshoter(),
//                                                         1048576);
//            cout<< container->size() << endl;
//cout << filteredOrder->blocks()->collect()->size() << endl;

            /*
            HashColumnJoin allJoin(0, Customer::CUSTKEY,
                                   new ColumnBuilder({JRR(Customer::NATION), JL(1), JL(2), JL(3)}), true);
            auto allJoined = allJoin.join(*orderWithSupp, *filteredCustomer);

//            std::cout << allJoined->size() << endl;

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*allJoined);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);*/
        }

        void executeQ3_1ColDebug() {
            auto customerTable = ParquetTable::Open(Customer::path,
                                                    {Customer::NATION, Customer::REGION, Customer::CUSTKEY});
            auto lineorderTable = ParquetTable::Open(LineOrder::path,
                                                     {LineOrder::CUSTKEY, LineOrder::SUPPKEY, LineOrder::ORDERDATE,
                                                      LineOrder::REVENUE});
            auto supplierTable = ParquetTable::Open(Supplier::path,
                                                    {Supplier::SUPPKEY, Supplier::NATION, Supplier::REGION});

            ColFilter supplierFilter(
                    new SBoostByteArrayPredicate(Supplier::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredSupplier = supplierFilter.filter(*supplierTable);

            ColFilter custFilter(
                    new SBoostByteArrayPredicate(Customer::REGION, bind(ByteArrayDictEq::build, region)));
            auto filteredCustomer = custFilter.filter(*customerTable);

            ColFilter orderFilter(
                    new SBoostByteArrayPredicate(LineOrder::ORDERDATE,
                                                 bind(ByteArrayDictBetween::build, date_from, date_to)));
            auto filteredOrder = orderFilter.filter(*lineorderTable);

            ParquetHashColumnJoin orderSupplierJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY, new WithNationColBuilder());
            // CUSTKEY, S_NATION, YEAR, REVENUE
            auto orderWithSupp = orderSupplierJoin.join(*filteredOrder, *filteredSupplier);

            cout << orderWithSupp->size() << endl;
//            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
//            printer.print(*orderWithSupp);

            /*
            HashColumnJoin allJoin(0, Customer::CUSTKEY,
                                   new ColumnBuilder({JRR(Customer::NATION), JL(1), JL(2), JL(3)}), true);
            auto allJoined = allJoin.join(*orderWithSupp, *filteredCustomer);

//            std::cout << allJoined->size() << endl;

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 22) + (data[1].asInt() << 12) + data[2].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(3)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)
                    ->field(F_REGULAR, 2, 2)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*allJoined);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(2) || (SIE(2) && SDGE(3));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PI(2) PD(3) PEND);
            printer.print(*sorted);
             */
        }
    }
}