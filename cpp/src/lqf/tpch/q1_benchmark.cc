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
#include "tpch_query.h"

using namespace std;
using namespace parquet;
using namespace lqf::sboost;
using namespace lqf::agg;

namespace lqf {
    namespace tpch {

        void executeQ1() {
            auto dateFrom = ByteArray("1998-09-01");
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
                    value_ += dataRow[LineItem::EXTENDEDPRICE].asDouble()
                              * (1 - dataRow[LineItem::DISCOUNT].asDouble());
                }
            };

            class Field4 : public DoubleSum {
            public:
                Field4() : DoubleSum(0) {}

                void reduce(DataRow &dataRow) {
                    value_ += dataRow[LineItem::EXTENDEDPRICE].asDouble()
                              * (1 - dataRow[LineItem::DISCOUNT].asDouble()) *
                              (1 + dataRow[LineItem::TAX].asDouble());
                }
            };

            function<unique_ptr<AggReducer>(DataRow &)> headerInit = [](DataRow &source) {
                auto reducer = unique_ptr<AggReducer>(new AggReducer(2, {
                        new IntSum(LineItem::QUANTITY),
                        new DoubleSum(LineItem::EXTENDEDPRICE),
                        new Field3(),
                        new Field4(),
                        new IntAvg(LineItem::QUANTITY),
                        new DoubleAvg(LineItem::EXTENDEDPRICE),
                        new DoubleAvg(LineItem::DISCOUNT),
                        new Count()
                }));
                reducer->header()[0] = source(LineItem::RETURNFLAG);
                reducer->header()[1] = source(LineItem::LINESTATUS);

                return reducer;
            };

            TableAgg agg([=]() {
                return make_shared<TableCore>(10, 8, indexer, headerInit);
            });
            auto agged = agg.agg(*filtered);
//
            SmallSort sort(SORTER2(0, 1));
            auto sorted = sort.sort(*agged);

            auto printer = Printer::Make(PBEGIN PINT(0) PINT(1) PINT(2) PDOUBLE(3)
                    PDOUBLE(4) PDOUBLE(5) PDOUBLE(6) PDOUBLE(7) PDOUBLE(8) PINT(9) PEND);
            printer->print(*sorted);
        }
    }
}

//
int main(int argc, char **argv) {
    using namespace std;
    using namespace std::chrono;
    auto start = system_clock::now();
    lqf::tpch::executeQ1();
    auto stop = system_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);

    cout << duration.count() << endl;
}