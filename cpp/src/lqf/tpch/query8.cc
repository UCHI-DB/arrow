//
// Created by harper on 4/4/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {

        namespace q8{

            ByteArray regionName("AMERICA");
            ByteArray dateFrom("1995-01-01");
            ByteArray dateTo("1996-12-31");
            ByteArray partType("ECONOMY ANODIZED STEEL");
            ByteArray nationName("GERMANY");

            class OrderJoinBuilder : public RowBuilder {
            public:
                OrderJoinBuilder() : RowBuilder(
                        {JL(LineItem::EXTENDEDPRICE), JL(LineItem::SUPPKEY), JL(LineItem::DISCOUNT),
                         JRS(Orders::ORDERDATE)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) override {
                    target[0] = udf::date2year(right[0].asByteArray());
                    target[1] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                    target[2] = left[LineItem::SUPPKEY].asInt();
                }
            };

            class NationFilterField : public agg::Sum<double, agg::AsDouble> {
            protected:
                int nationKey_;
            public:
                NationFilterField(int nationKey) : agg::DoubleSum(1), nationKey_(nationKey) {}

                void reduce(DataRow &input) override {
                    *value_ += (input[2].asInt() == nationKey_) ? input[1].asDouble() : 0;
                }
            };
        }

        using namespace q8;
        void executeQ8() {

            auto region = ParquetTable::Open(Region::path, {Region::NAME, Region::REGIONKEY});
            auto nation = ParquetTable::Open(Nation::path, {Nation::REGIONKEY, Nation::NATIONKEY});
            auto customer = ParquetTable::Open(Customer::path, {Customer::NATIONKEY, Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERDATE});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::ORDERKEY, LineItem::PARTKEY, LineItem::DISCOUNT,
                                                LineItem::EXTENDEDPRICE, LineItem::SUPPKEY});
            auto part = ParquetTable::Open(Part::path, {Part::TYPE, Part::PARTKEY});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});


            ColFilter regionFilter({new SimplePredicate(Region::NAME, [=](const DataField &input) {
                return input.asByteArray() == regionName;
            })});
            auto validRegion = regionFilter.filter(*region);

            HashFilterJoin nationFilter(Nation::REGIONKEY, Region::REGIONKEY);
            auto validNation = nationFilter.join(*nation, *validRegion);

            HashFilterJoin customerFilter(Customer::NATIONKEY, Nation::NATIONKEY);
            auto validCust = customerFilter.join(*customer, *validNation);

            using namespace sboost;
            ColFilter orderDateFilter({new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                          bind(&ByteArrayDictBetween::build, dateFrom,
                                                                               dateTo))});
            auto validOrder = orderDateFilter.filter(*order);

            HashFilterJoin orderCustFilter(Orders::CUSTKEY, Customer::CUSTKEY);
            validOrder = orderCustFilter.join(*validOrder, *validCust);

            ColFilter partFilter(
                    {new SboostPredicate<ByteArrayType>(Part::TYPE, bind(&ByteArrayDictEq::build, partType))});
            auto validPart = partFilter.filter(*part);

            HashFilterJoin lineitemOnPartFilter(LineItem::PARTKEY, Part::PARTKEY);
            auto validLineitem = lineitemOnPartFilter.join(*lineitem, *validPart);


            HashJoin orderJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new OrderJoinBuilder());
            auto itemWithOrder = orderJoin.join(*validLineitem, *validOrder);

            HashColumnJoin itemWithSupplierJoin(2, Supplier::SUPPKEY,
                                                new ColumnBuilder({JL(0), JL(1), JR(Supplier::NATIONKEY)}));
            auto result = itemWithSupplierJoin.join(*itemWithOrder, *supplier);

            function<uint64_t(DataRow &)> hasher = [](DataRow &input) {
                return input[0].asInt();
            };

            int nationKey2 = KeyFinder(Nation::NATIONKEY, [=](DataRow &row) {
                return row[Nation::NAME].asByteArray() == nationName;
            }).find(*nation);


            HashAgg agg(vector<uint32_t>({1, 1}), {AGI(0)},
                        [=]() {
                            return vector<AggField *>({new agg::DoubleSum(1), new NationFilterField(nationKey2)});
                        }, hasher);
            result = agg.agg(*result);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*result);

            auto printer = Printer::Make(PBEGIN PI(0) PD(1) PD(2) PEND);
            printer->print(*sorted);
        }
    }
}