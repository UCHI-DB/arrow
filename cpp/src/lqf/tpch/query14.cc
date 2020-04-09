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
#include <lqf/util.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {
        namespace q14 {

            ByteArray dateFrom("1995-09-01");
            ByteArray dateTo("1995-10-01");
            const char *prefix = "PROMO";

            class ItemBuilder : public RowBuilder {
            public:
                ItemBuilder() : RowBuilder({JL(LineItem::EXTENDEDPRICE)}, false) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = left[LineItem::EXTENDEDPRICE].asDouble() * (1 - left[LineItem::DISCOUNT].asDouble());
                    if (&right == &MemDataRow::EMPTY) {
                        target[1] = 0;
                    } else {
                        target[1] = target[0].asDouble();
                    }
                }
            };
        }

        using namespace q14;

        void executeQ14() {

            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SHIPDATE, LineItem::PARTKEY, LineItem::EXTENDEDPRICE,
                                                LineItem::DISCOUNT});
            auto part = ParquetTable::Open(Part::path, {Part::PARTKEY, Part::TYPE});

            using namespace sboost;
            function<bool(const ByteArray &)> filter = [](const ByteArray &val) {
                const char *begin = (const char *) val.ptr;
                return lqf::util::strnstr(begin, prefix, val.len) == begin;
            };
            ColFilter partFilter({new SboostPredicate<ByteArrayType>(Part::TYPE,
                                                                     bind(&ByteArrayDictMultiEq::build, filter))});
            auto validPart = partFilter.filter(*part);

            ColFilter lineitemShipdateFilter({new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                                 bind(&ByteArrayDictRangele::build,
                                                                                      dateFrom, dateTo))});
            auto validLineitem = lineitemShipdateFilter.filter(*lineitem);


            HashJoin join(LineItem::PARTKEY, Part::PARTKEY, new ItemBuilder());
            join.useOuter();
            auto lineitemWithPartType = join.join(*validLineitem, *validPart);

            using namespace agg;
            SimpleAgg simpleAgg(vector<uint32_t>(1, 1),
                                []() { return vector<AggField *>{new DoubleSum(0), new DoubleSum(1)}; });
            auto result = simpleAgg.agg(*lineitemWithPartType);

            auto printer = Printer::Make(PBEGIN PD(0) PD(1) PEND);
            printer->print(*result);
        }

    }
}