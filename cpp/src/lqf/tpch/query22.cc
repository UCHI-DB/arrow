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
        namespace q22 {
            unordered_set<string> phones{"31", "13", "23", "29", "30", "18", "17"};

            class PositiveAvg : public DoubleAvg {
            public:
                PositiveAvg() : DoubleAvg(Customer::ACCTBAL) {}

                void reduce(DataRow &row) override {
                    if (row[Customer::ACCTBAL].asDouble() > 0) {
                        DoubleAvg::reduce(row);
                    }
                }
            };

            vector<uint32_t> col_offset{0, 2, 3};

            class PhoneBuilder : public JoinBuilder {
            public:
                PhoneBuilder() : JoinBuilder({JRS(Customer::PHONE), JR(Customer::ACCTBAL)}, false, false) {}

                unique_ptr<MemDataRow> snapshot(DataRow &right) override {
                    MemDataRow *result = new MemDataRow(col_offset);
                    (*result)[0] = right[Customer::PHONE].asByteArray();
                    (*result)[0].asByteArray().len = 2;
                    (*result)[1] = right[Customer::ACCTBAL].asDouble();
                    return unique_ptr<MemDataRow>(result);
                }
            };
        }

        using namespace q22;

        void executeQ22Debug() {
            auto customer = ParquetTable::Open(Customer::path, {Customer::PHONE, Customer::ACCTBAL, Customer::CUSTKEY});
            customer->blocks()->foreach([](const shared_ptr<Block> &block) {
                auto phonecol = block->col(Customer::PHONE);
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    auto field = phonecol->next();
                    if (i == 110381) {
                        cout << field << endl;
                    }
                }
            });
        }

        void executeQ22() {
            auto customer = ParquetTable::Open(Customer::path, {Customer::PHONE, Customer::ACCTBAL, Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::CUSTKEY});

            ColFilter custFilter(new SimplePredicate(Customer::PHONE, [](const DataField &field) {
                ByteArray &val = field.asByteArray();
                return phones.find(string((const char *) val.ptr, 2)) != phones.end();
            }));
            auto validCust = FilterMat().mat(*custFilter.filter(*customer));

            SimpleAgg avgagg(vector<uint32_t>{1}, []() { return vector<AggField *>{new PositiveAvg()}; });
            auto avgCust = avgagg.agg(*validCust);
            double avg = (*(*avgCust->blocks()->collect())[0]).rows()->next()[0].asDouble();

            ColFilter avgFilter(new SimplePredicate(Customer::ACCTBAL, [=](const DataField &field) {
                return field.asDouble() > avg;
            }));
            auto filteredCust = avgFilter.filter(*validCust);
//            cout << filteredCust->size() << endl;

            HashNotExistJoin notExistJoin(Orders::CUSTKEY, 0, new PhoneBuilder());
            // PHONE, ACCTBAL
            auto noorderCust = notExistJoin.join(*order, *filteredCust);

            function<uint64_t(DataRow &)> hasher = [](DataRow &input) {
                ByteArray &val = input[0].asByteArray();
                return (static_cast<int64_t>(val.ptr[0]) << 8) + val.ptr[1];
            };
            HashAgg agg(vector<uint32_t>{2, 1, 1}, {AGB(0)},
                        []() { return vector<AggField *>{new DoubleSum(1), new Count()}; }, hasher);
            auto result = agg.agg(*noorderCust);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0);
            };
            SmallSort sorter(comparator);
            auto sorted = sorter.sort(*result);

            Printer printer(PBEGIN PB(0) PD(1) PI(2) PEND);
            printer.print(*sorted);
        }
    }
}