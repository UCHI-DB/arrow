//
// Created by harper on 3/3/20.
//
#include <iostream>
#include <chrono>
#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include "tpchquery.h"

using namespace std;
using namespace parquet;
using namespace lqf::sboost;
using namespace lqf::agg;

namespace lqf {
    namespace tpch {
        namespace q1 {
            ByteArray dateFrom("1998-09-01");
        }

        using namespace q1;

        void executeQ1() {
            auto lineItemTable = ParquetTable::Open(LineItem::path,
                                                    {LineItem::SHIPDATE, LineItem::QUANTITY, LineItem::EXTENDEDPRICE,
                                                     LineItem::DISCOUNT, LineItem::TAX, LineItem::RETURNFLAG,
                                                     LineItem::LINESTATUS});


            // Use SBoost Filter
            ColFilter colFilter(
                    {new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE, bind(ByteArrayDictLess::build, dateFrom))});
            auto filtered = colFilter.filter(*lineItemTable);

            function<uint32_t(DataRow &row)> indexer = [](DataRow &row) {
                return (row(LineItem::RETURNFLAG).asInt() << 1) + row(LineItem::LINESTATUS).asInt();
            };

            class Field3 : public DoubleSum {
            public:
                Field3() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    *value_ += dataRow[LineItem::EXTENDEDPRICE].asDouble()
                               * (1 - dataRow[LineItem::DISCOUNT].asDouble());
                }
            };

            class Field4 : public DoubleSum {
            public:
                Field4() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    *value_ += dataRow[LineItem::EXTENDEDPRICE].asDouble()
                               * (1 - dataRow[LineItem::DISCOUNT].asDouble()) *
                               (1 + dataRow[LineItem::TAX].asDouble());
                }
            };

            function<vector<AggField *>()> aggFields = []() {
                return vector<AggField *>{
                        new IntSum(LineItem::QUANTITY),
                        new DoubleSum(LineItem::EXTENDEDPRICE),
                        new Field3(),
                        new Field4(),
                        new IntAvg(LineItem::QUANTITY),
                        new DoubleAvg(LineItem::EXTENDEDPRICE),
                        new DoubleAvg(LineItem::DISCOUNT),
                        new Count()
                };
            };

            TableAgg agg(lqf::colSize(10),
                         {AGR(LineItem::RETURNFLAG), AGR(LineItem::LINESTATUS)},
                         aggFields, 10, indexer);
            auto agged = agg.agg(*filtered);
//
            SmallSort sort(SORTER2(0, 1));
            auto sorted = sort.sort(*agged);

            auto printer = Printer::Make(PBEGIN PI(0)
                    PI(1)
                    PI(2)
                    PD(3)
                    PD(4)
                    PD(5)
                    PD(6)
                    PD(7)
                    PD(8)
                    PI(9)
            PEND);
            printer->print(*sorted);
        }
    }
}

//
//int main(int argc, char **argv) {
//    using namespace std;
//    using namespace std::chrono;
//    auto start = system_clock::now();
//    lqf::tpch::executeQ1();
//    auto stop = system_clock::now();
//    auto duration = duration_cast<microseconds>(stop - start);
//
//    cout << duration.count() << endl;
//}