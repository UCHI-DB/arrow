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

            HashNotExistJoin

            Join notExistJoin = new HashNotExistJoin(Orders.CUSTKEY, 0);
            Table noorderCust = notExistJoin.join(order, bigCust);

            Agg countSum = new KeyAgg(new KeyReducerSource() {
                @Override
                public long applyAsLong(DataRow from) {
                    return Integer.parseInt(from.getBinary(2).toStringUsingUTF8());
                }

                @Override
                public RowReducer apply(DataRow dataRow) {
                    FullDataRow fd = new FullDataRow(new int[]{1, 1, 1});
                    fd.setBinary(2, dataRow.getBinary(2));
                    return new FieldsReducer(fd, new DoubleSum(1, 1), new Count(0));
                }
            });
            Table result = countSum.agg(noorderCust);

            Sort sorter = new MemSort(Comparator.comparing(row->row.getBinary(2).toStringUsingUTF8()));
            result = sorter.sort(result);

            new Printer.DefaultPrinter(row->System.out.println(row.getBinary(2).toStringUsingUTF8()
                                                               + "," + row.getInt(0) + "," + row.getDouble(1))).print(
                    result);

            System.out.println(System.currentTimeMillis() - startTime);
        }
    }
}