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

        void executeQ9() {
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::NAME});
            auto partsupp = ParquetTable::Open(PartSupp::path,
                                               {PartSupp::PARTKEY, PartSupp::SUPPKEY, PartSupp::SUPPLYCOST});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::PARTKEY, LineItem::DISCOUNT, LineItem::EXTENDEDPRICE,
                                                LineItem::QUANTITY, LineItem::SUPPKEY, LineItem::ORDERKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERDATE});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY});
            auto nation = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME});

            ColFilter partFilter({new SimplePredicate(Part::NAME, [](const DataField &field) {
                auto &ref = field.asByteArray();
                string str((const char *) ref.ptr, ref.len);
                return str.find("green");
            })});
            auto validPart = partFilter.filter(*part);

            HashFilterJoin itemPartJoin(LineItem::PARTKEY, Part::PARTKEY);
            auto validLineitem = itemPartJoin.join(*lineitem, *validPart);

            class ItemWithOrderBuilder : public RowBuilder {

            public:
                ItemWithOrderBuilder() : RowBuilder({JL(LineItem::PARTKEY), JL(LineItem::SUPPKEY),
                                                     JL(LineItem::EXTENDEDPRICE), JL(LineItem::DISCOUNT),
                                                     JL(LineItem::QUANTITY), JRS(Orders::ORDERDATE)}, false, true) {}


                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[LineItem::PARTKEY].asInt();
                    target[1] = left[LineItem::SUPPKEY].asInt();
                    target[2] = left[LineItem::EXTENDEDPRICE].asDouble();
                    target[3] = left[LineItem::DISCOUNT].asDouble();
                    target[4] = left[LineItem::QUANTITY].asInt();
                    target[5] = udf::date2year(right[0].asByteArray());
                }
            };
            HashJoin itemOrderJoin(LineItem::ORDERKEY, Orders::ORDERKEY, new ItemWithOrderBuilder());
            auto orderLineitem = itemOrderJoin.join(*validLineitem, *order);

            // SUPPKEY, ORDER_YEAR, REV
            class ItemWithRevBuilder : public RowBuilder {
            public :
                ItemWithRevBuilder() : RowBuilder({JL(0), JL(5), JR(PartSupp::SUPPLYCOST)}, false, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[1].asInt();
                    target[1] = left[5].asInt();
                    target[2] = left[2].asDouble() * (1 - left[3].asDouble()) - right[0].asDouble() * target[4].asInt();
                }
            };
            using namespace powerjoin;
            function<uint64_t(DataRow &)> lineitem_key_maker = [](DataRow &row) {
                return (static_cast<uint64_t>(row[LineItem::PARTKEY].asInt()) << 32) + row[LineItem::SUPPKEY].asInt();
            };
            function<uint64_t(DataRow &)> partsupp_key_maker = [](DataRow &row) {
                return (static_cast<uint64_t>(row[PartSupp::PARTKEY].asInt()) << 32) + row[PartSupp::SUPPKEY].asInt();
            };
            PowerHashJoin ps2lJoin(lineitem_key_maker, partsupp_key_maker, new ItemWithRevBuilder());
            auto itemWithRev = ps2lJoin.join(*orderLineitem, *partsupp);

            // ORDER_YEAR, REV, NATIONKEY
            HashColumnJoin suppJoin(0, Supplier::SUPPKEY, new ColumnBuilder({JL(1), JL(2), JR(Supplier::NATIONKEY)}));
            auto itemWithNation = suppJoin.join(*itemWithRev, *supplier);

            function<uint64_t(DataRow &)> hasher = [](DataRow &input) {
                return (static_cast<uint64_t>(input[0].asInt()) << 32) + input[2].asInt();
            };
            HashAgg agg(vector<uint32_t>{1, 1, 1}, {AGI(2), AGI(0)},
                        []() { return vector<AggField *>({new agg::DoubleSum(1)}); }, hasher, true);
            // NATIONKEY, ORDERYEAR, SUM
            auto result = agg.agg(*itemWithNation);

            HashJoin withNationJoin(0, Nation::NATIONKEY,
                                                   new RowBuilder({JL(1), JRS(Nation::NAME), JL(2)}, false, true));
            // ORDERYEAR, NATION, REV
            result = withNationJoin.join(*result, *nation);

            function<bool(DataRow*,DataRow*)> comp = [](DataRow* a, DataRow*b) {
               return SBLE(1) || (SBE(1) && SILE(0));
            };
            SmallSort sort(comp);
            result = sort.sort(*result);

            auto printer = Printer::Make(PBEGIN PI(0) PB(1) PD(2) PEND);
            printer->print(*result);
        }
    }
}