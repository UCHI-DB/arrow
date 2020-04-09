//
// Created by harper on 4/6/20.
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
        using namespace agg;
        namespace q12 {
            ByteArray highp1("1-URGENT");
            ByteArray highp2("2-HIGH");
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");
            ByteArray mode1("MAIL");
            ByteArray mode2("SHIP");

            class OrderHighPriorityField : public IntSum {
            public:
                OrderHighPriorityField() : IntSum(0) {}

                void reduce(DataRow &input) override {
                    int32_t orderpriority = input[2].asInt();
                    if (orderpriority == 0 || orderpriority == 1) {
                        *value_ += input[1].asInt();
                    }
                }
            };

            class OrderLowPriorityField : public IntSum {
            public:
                OrderLowPriorityField() : IntSum(0) {}

                void reduce(DataRow &input) override {
                    int32_t orderpriority = input[2].asInt();
                    if (!(orderpriority == 0 || orderpriority == 1)) {
                        *value_ += input[1].asInt();
                    }
                }
            };
        }
        using namespace q12;

        void executeQ12() {

            auto lineitem = ParquetTable::Open(LineItem::path, {LineItem::RECEIPTDATE, LineItem::SHIPMODE});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERPRIORITY});

            using namespace sboost;
            ColFilter lineitemFilter({new SboostPredicate<ByteArrayType>(LineItem::RECEIPTDATE,
                                                                         bind(&ByteArrayDictRangele::build,
                                                                              dateFrom, dateTo)),
                                      new SboostPredicate<ByteArrayType>(LineItem::SHIPMODE,
                                                                         bind(&ByteArrayDictMultiEq::build,
                                                                              [=](const ByteArray &data) {
                                                                                  return data == mode1 ||
                                                                                         data == mode2;
                                                                              }))});
            auto validLineitem = lineitemFilter.filter(*lineitem);

            RowFilter lineItemAgainFilter([](DataRow &row) {
                auto &commitDate = row[LineItem::COMMITDATE].asByteArray();
                auto &shipDate = row[LineItem::SHIPDATE].asByteArray();
                auto &receiptDate = row[LineItem::RECEIPTDATE].asByteArray();
                return commitDate < receiptDate && shipDate < commitDate;
            });
            validLineitem = lineItemAgainFilter.filter(*validLineitem);

            function<uint64_t(DataRow &)> hasher = [](DataRow &row) {
                return (row[LineItem::ORDERKEY].asInt() << 3) + row(LineItem::SHIPMODE).asInt();
            };

            HashAgg lineitemAgg(vector<uint32_t>({1, 1, 1}), {AGI(LineItem::ORDERKEY), AGR(LineItem::SHIPMODE)},
                                []() { return vector<AggField *>({new agg::Count()}); }, hasher);
            lineitemAgg.useVertical();
            // ORDER_KEY, SHIPMODE, COUNT
            auto agglineitem = lineitemAgg.agg(*validLineitem);

            HashColumnJoin join(0, Orders::ORDERKEY, new ColumnBuilder({JL(1), JL(2), JRR(Orders::ORDERPRIORITY)}));
            // SHIPMODE, COUNT, ORDERPRIORITY
            auto itemwithorder = join.join(*agglineitem, *order);

            using namespace agg;

            HashAgg finalAgg(vector<uint32_t>{1, 1, 1}, {AGI(0)}, []() {
                return vector<AggField *>{new OrderHighPriorityField(), new OrderLowPriorityField()};
            }, COL_HASHER(0));
            auto result = finalAgg.agg(*itemwithorder);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(0);
            };
            SmallSort sorter(comparator);
            auto sorted = sorter.sort(*result);

            auto printer = Printer::Make(PBEGIN PI(0) PI(1) PI(2) PEND);
            printer->print(*sorted);
        }
    }
}