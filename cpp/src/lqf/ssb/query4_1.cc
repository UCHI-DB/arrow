//
// Created by Harper on 1/3/21.
//

#include "query4.h"

namespace lqf {
    namespace ssb {
        namespace q4_1 {
            class OrderProfitBuilder : public RowBuilder {
            public:
                OrderProfitBuilder() : RowBuilder(
                        {JL(LineOrder::ORDERDATE), JL(LineOrder::REVENUE), JL(LineOrder::SUPPLYCOST),
                         JRR(Customer::NATION)}, false, false) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = udf::date2year(left[LineOrder::ORDERDATE].asByteArray());
                    target[1] = right[0].asInt();
                    target[2] = left[LineOrder::REVENUE].asDouble() - left[LineOrder::SUPPLYCOST].asDouble();
                }
            };

            ByteArray region("AMERICA");
            ByteArray mfgr1("MFGR#1");
            ByteArray mfgr2("MFGR#2");
        }

        using namespace q4;
        using namespace q4_1;
        using namespace parallel;
        using namespace sboost;

        void executeQ4_1Plain() {
            auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::NATION});
            auto partTable = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::REGION});
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

            FilterJoin suppFilterJoin(LineOrder::SUPPKEY, Supplier::SUPPKEY);
            auto orderOnValidSupp = suppFilterJoin.join(*lineorderTable, *filteredSupp);

            FilterTJoin<Hash32SetPredicate> partFilterJoin(LineOrder::PARTKEY, Part::PARTKEY);
            auto orderOnValidSP = partFilterJoin.join(*orderOnValidSupp, *filteredPart);

            HashJoin withCustJoin(LineOrder::CUSTKEY, Customer::CUSTKEY, new OrderProfitBuilder());
            auto validOrder = withCustJoin.join(*orderOnValidSP, *filteredCustomer);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 10) + data[1].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(2)};
            };
            HashAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->buildSnapshot(), aggFields);
            auto agged = agg.agg(*validOrder);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SDLE(1));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            // TODO use dictionary to print column 1
            Printer printer(PBEGIN PI(0) PI(1) PD(2) PEND);
            printer.print(*sorted);
        }

        void executeQ4_1() {
            ExecutionGraph graph;

            auto customer = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::NATION});
            auto part = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::REGION});
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

            auto suppFilterJoin = graph.add(new FilterTJoin<Hash32SetPredicate>(LineOrder::SUPPKEY, Supplier::SUPPKEY),
                                            {lineorderTable, suppFilter});
            auto partFilterJoin = graph.add(new FilterTJoin<Hash32SetPredicate>(LineOrder::PARTKEY, Part::PARTKEY),
                                            {suppFilterJoin, partFilter});
            auto withCustJoin = graph.add(new HashTJoin<Hash32SparseContainer>(LineOrder::CUSTKEY, Customer::CUSTKEY,
                                                                               new OrderProfitBuilder()),
                                          {partFilterJoin, custFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 10) + data[1].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(2)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->buildSnapshot(), aggFields), {withCustJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SDLE(1));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PI(0) PI(1) PD(2) PEND), {sort});

            graph.execute(true);
        }

        void executeQ4_1Enc() {
            ExecutionGraph graph;

            auto customer = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::NATION});
            auto part = ParquetTable::Open(Part::path, {Part::MFGR, Part::PARTKEY});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::REGION});
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

            auto suppFilterJoin = graph.add(new FilterTJoin<Hash32SetPredicate>(LineOrder::SUPPKEY, Supplier::SUPPKEY),
                                            {lineorderTable, suppFilter});
            auto partFilterJoin = graph.add(new FilterTJoin<Hash32SetPredicate>(LineOrder::PARTKEY, Part::PARTKEY),
                                            {suppFilterJoin, partFilter});
            auto withCustJoin = graph.add(new encopr::EncHashTJoin<Hash32SparseContainer>(
                    LineOrder::CUSTKEY, Customer::CUSTKEY, new OrderProfitBuilder(),
                    {parquet::Type::type::INT32, parquet::Type::type::INT32, parquet::Type::type::DOUBLE},
                    {encoding::EncodingType::DICTIONARY, encoding::EncodingType::DICTIONARY,
                     encoding::EncodingType::PLAIN}),
                                          {partFilterJoin, custFilter});

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return (data[0].asInt() << 10) + data[1].asInt();
            };
            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{new DoubleSum(2)};
            };
            auto agg = graph.add(new HashAgg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)
                    ->field(F_REGULAR, 1, 1)->buildSnapshot(), aggFields), {withCustJoin});

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0) || (SIE(0) && SDLE(1));
            };
            auto sort = graph.add(new SmallSort(comparator), {agg});

            // TODO use dictionary to print column 1
            graph.add(new Printer(PBEGIN PI(0) PI(1) PD(2) PEND), {sort});

            graph.execute(true);
        }
    }
}
