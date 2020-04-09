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

        unordered_set<string> phones{"31", "13", "23", "29", "30", "18", "17"};

        void executeQ22() {

            auto customer = ParquetTable::Open(Customer::path, {Customer::PHONE, Customer::ACCTBAL, Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::CUSTKEY});

            ColFilter custFilter(new SimplePredicate(Customer::PHONE, [](const DataField &field) {
                ByteArray &val = field.asByteArray();
                return phones.find(string((const char *) val.ptr, 2)) != phones.end();
            }));
            auto validCust = FilterMat().mat(*custFilter.filter(*customer));

            using namespace agg;
            class PositiveAvg : public DoubleAvg {
            public:
                PositiveAvg() : DoubleAvg(Customer::ACCTBAL) {}

                void reduce(DataRow &row) override {
                    if (row[Customer::ACCTBAL].asDouble() > 0) {
                        DoubleAvg::reduce(row);
                    }
                }
            };
            SimpleAgg avgagg(vector<uint32_t>{}, []() { return vector<AggField *>{new PositiveAvg()}; });
            auto avgCust = avgagg.agg(*validCust);
            double avg = (*(*avgCust->blocks()->collect())[0]).rows()->next()[0].asDouble();

            ColFilter avgFilter(new SimplePredicate(Customer::ACCTBAL, [=](const DataField &field) {
                return field.asDouble() > avg;
            }));
            auto filteredCust = avgFilter.filter(*validCust);

            HashNotExistJoin notExistJoin(Orders::CUSTKEY, 0,
                                          new RowBuilder({{JR(Customer::PHONE), JR(Customer::ACCTBAL)}}));
            // PHONE, ACCTBAL
            auto noorderCust = notExistJoin.join(*order, *filteredCust);

            using namespace agg;

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
            result = sorter.sort(*result);

            auto printer = Printer::Make(PBEGIN PB(0) PD(1) PI(2) PEND);
            printer->print(*result);
        }
    }
}