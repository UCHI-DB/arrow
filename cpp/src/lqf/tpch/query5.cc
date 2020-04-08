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

        void executeQ5() {
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");
            ByteArray region("ASIA");

            auto regionTable = ParquetTable::Open(Region::path, {Region::REGIONKEY, Region::NAME});
            auto nationTable = ParquetTable::Open(Nation::path, {Nation::REGIONKEY, Nation::NAME, Nation::NATIONKEY});
            auto supplierTable = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY});
            auto customerTable = ParquetTable::Open(Customer::path, {Customer::NATIONKEY});
            auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY, Orders::ORDERDATE});
            auto lineitemTable = ParquetTable::Open(LineItem::path, {LineItem::DISCOUNT, LineItem::EXTENDEDPRICE,
                                                                     LineItem::ORDERKEY, LineItem::SUPPKEY});

            ColFilter regionFilter(
                    {new SimplePredicate(Region::NAME,
                                         [=](const DataField &df) { return df.asByteArray() == region; })});
            auto validRegion = regionFilter.filter(*regionTable);

            HashFilterJoin nationFilterJoin(Nation::REGIONKEY, Region::REGIONKEY);
            auto validNation = nationFilterJoin.join(*nationTable, *validRegion);

            vector<uint32_t> nation_col_offset{0, 2};
            function<unique_ptr<MemDataRow>(DataRow &)> snapshot = [&nation_col_offset](DataRow &input) {
                auto result = new MemDataRow(nation_col_offset);
                (*result)[0] = input[Nation::NAME];
                return unique_ptr<MemDataRow>(result);
            };
            auto matNation = HashMat(Nation::NATIONKEY, snapshot).mat(*validNation);

            HashFilterJoin custNationFilter(Customer::NATIONKEY, Nation::NATIONKEY);
            auto validCustomer = custNationFilter.join(*customerTable, *matNation);

            HashFilterJoin suppNationFilter(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto validSupplier = suppNationFilter.join(*supplierTable, *matNation);

            using namespace lqf::sboost;
            ColFilter orderFilter({new SboostPredicate<ByteArrayType>(Orders::ORDERDATE,
                                                                      bind(&ByteArrayDictRangele::build, dateFrom,
                                                                           dateTo))});
            auto validOrder = orderFilter.filter(*orderTable);

            HashColumnJoin orderOnCustomerJoin(Orders::CUSTKEY, Customer::CUSTKEY,
                                               new ColumnBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)}));
            validOrder = orderOnCustomerJoin.join(*validOrder, *validCustomer);

            class ItemPriceRowBuilder : public RowBuilder {
            public:
                ItemPriceRowBuilder() : RowBuilder({}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int32_t key) override {
                    target[0] = left[LineItem::ORDERKEY];
                    target[1] = right[Supplier::NATIONKEY];
                    target[2] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                }
            };
            HashJoin itemOnSupplierJoin(LineItem::SUPPKEY, Supplier::SUPPKEY, new ItemPriceRowBuilder());
            auto validLineitem = itemOnSupplierJoin.join(*lineitemTable, *validSupplier);

            function<uint64_t(DataRow &)> key_maker = [](DataRow &dr) {
                return (static_cast<uint64_t>(dr[0].asInt()) << 32) + dr[1].asInt();
            };
            using namespace powerjoin;
            PowerHashJoin orderItemJoin(key_maker, key_maker, new RowBuilder({JL(1), JR(2)}));
            auto joined = orderItemJoin.join(*validLineitem, *validOrder);

            TableAgg agg(vector<uint32_t>{1, 1}, {AGI(0)}, []() { return vector<AggField *>{new agg::DoubleSum(1)}; },
                         30, [](DataRow &dr) { return dr[0].asInt(); });
            agg.useVertical();
            auto agged = agg.agg(*joined);

            HashColumnJoin addNationNameJoin(0, 0, new ColumnBuilder({JL(1), JRS(1)}));
            auto result = addNationNameJoin.join(*agged, *matNation);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return (*a)[1].asByteArray() < (*b)[1].asByteArray();
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*result);

            auto printer = Printer::Make(PBEGIN PD(0) PB(1) PEND);
            printer->print(*sorted);
        }
    }
}