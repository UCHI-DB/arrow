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

        void executeQ7() {
            string nation1("FRANCE");
            string nation2("GERMANY");

            ByteArray dateFrom("1995-01-01");
            ByteArray dateTo("1996-12-31");

            auto nationTable = ParquetTable::Open(Nation::path);
            vector<uint32_t> nation_col_size({1, 2});
            function<unique_ptr<MemDataRow>(DataRow &)> snapshot = [&nation_col_size](DataRow &dr) {
                auto row = new MemDataRow(nation_col_size);
                (*row)[0] = dr[Nation::NAME].asByteArray();
                return unique_ptr<MemDataRow>(row);
            };
            auto matNation = HashMat(Nation::NATIONKEY, snapshot).mat(*nationTable);

            auto customerTable = ParquetTable::Open(Customer::path);
            auto supplierTable = ParquetTable::Open(Supplier::path);
            auto orderTable = ParquetTable::Open(Orders::path);
            auto lineitemTable = ParquetTable::Open(LineItem::path);

            unordered_map<string, int32_t> nation_mapping;
            nationTable->blocks()->foreach([&nation_mapping](const shared_ptr<Block> &block) {
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    stringstream ks;
                    ks << row[Nation::NAME].asByteArray();
                    nation_mapping[ks.str()] = row[Nation::NATIONKEY].asInt();
                }
            });

            int nationKey1 = nation_mapping[nation1];
            int nationKey2 = nation_mapping[nation2];

            using namespace sboost;

            function<bool(int32_t)> nation_key_pred = [=](int32_t key) {
                return key == nationKey1 ||
                       key == nationKey2;
            };
            ColFilter validCustomerFilter({new SboostPredicate<Int32Type>(Customer::NATIONKEY,
                                                                          bind(&Int32DictMultiEq::build,
                                                                               nation_key_pred))});
            auto validCustomer = validCustomerFilter.filter(*customerTable);

            ColFilter validSupplierFilter({new SboostPredicate<Int32Type>(Customer::NATIONKEY,
                                                                          bind(&Int32DictMultiEq::build,
                                                                               nation_key_pred))});
            auto validSupplier = validSupplierFilter.filter(*supplierTable);


            HashJoin orderWithNationJoin(Orders::CUSTKEY, Customer::CUSTKEY,
                                         new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)}, true));
            auto orderWithNation = orderWithNationJoin.join(*orderTable, *validCustomer);

            ColFilter lineitemFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                         bind(&ByteArrayDictBetween::build, dateFrom,
                                                                              dateTo))});
            auto filteredLineitem = lineitemFilter.filter(*lineitemTable);

            class Q7ItemWithNationBuilder : public RowBuilder {
            public:
                Q7ItemWithNationBuilder() : RowBuilder(
                        {JL(LineItem::ORDERKEY), JR(Supplier::NATIONKEY), JL(0), JL(LineItem::EXTENDEDPRICE)}, false,
                        true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[LineItem::ORDERKEY];
                    target[1] = right[Supplier::NATIONKEY];
                    target[2] = udf::date2year(left[LineItem::SHIPDATE].asByteArray());
                    target[3] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                }
            };
            HashJoin itemWithNationJoin(LineItem::SUPPKEY, Supplier::SUPPKEY, new Q7ItemWithNationBuilder());
            auto itemWithNation = itemWithNationJoin.join(*filteredLineitem, *validSupplier);

            HashJoin itemWithOrderJoin(0, 0, new RowBuilder({JL(1), JR(1), JL(2), JL(3)}, true),
                                       [](DataRow &left, DataRow &right) {
                                           return left[1].asInt() != right[1].asInt();
                                       });
            // Customer::NATIONKEY, Supplier::NATIONKEY, YEAR, PRICE
            auto joined = itemWithOrderJoin.join(*itemWithNation, *orderWithNation);

            function<uint64_t(DataRow &)> indexer = [](DataRow &input) {
                return (input[0].asInt() << 16) + (input[1].asInt() << 11) + input[2].asInt();
            };
            HashAgg agg(vector<uint32_t>({1, 1, 1, 1}), {AGI(0), AGI(1), AGI(2)},
                        []() { return vector<AggField *>({new agg::DoubleSum(3)}); }, indexer);
            agg.useVertical();
            auto agged = agg.agg(*joined);

            HashColumnJoin joinNation1(0, 0, new ColumnBuilder({JRS(1), JL(1), JL(2), JL(3)}));
            agged = joinNation1.join(*agged, *matNation);

            HashColumnJoin joinNation2(0, 0, new ColumnBuilder({JLS(0), JRS(1), JL(2), JL(3)}));
            agged = joinNation2.join(*agged, *matNation);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0) || (SBE(0) && SBLE(1)) || (SBE(0) && SBE(1) && SILE(2));
            };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*agged);

            auto printer = Printer::Make(PBEGIN PB(0) PB(1) PI(2) PD(3) PEND);
            printer->print(*sorted);
        }
    }
}