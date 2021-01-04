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
                    target[1] = right(Customer::NATION).asInt();
                    target[2] = left[LineOrder::REVENUE].asDouble() - left[LineOrder::SUPPLYCOST].asDouble();
                }
            };

            ByteArray region("AMERICA");
            ByteArray mfgr1("MFGR#1");
            ByteArray mfgr2("MFGR#2");
        }

        using namespace q4;
        using namespace q4_1;
        using namespace sboost;

        void executeQ4_1() {
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

            FilterJoin partFilterJoin(LineOrder::PARTKEY, Part::PARTKEY);
            auto orderOnValidSP = partFilterJoin.join(*orderOnValidSupp, *filteredPart);

            HashJoin withCustJoin(LineOrder::CUSTKEY, Customer::CUSTKEY, new OrderProfitBuilder());
            auto validOrder = withCustJoin.join(*orderOnValidSP, *filteredCustomer);

            function<uint64_t(DataRow &)> hasher = [](DataRow &data) {
                return data[0].asInt() << 10 + data[1].asInt();
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
            printer.print(*sorted);;
        }
    }
}
