//
// Created by harper on 4/6/20.
//

#include <string.h>
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

        void executeQ13() {
            auto customer = ParquetTable::Open(Customer::path, {Customer::CUSTKEY});
            auto order = ParquetTable::Open(Orders::path, {Orders::COMMENT, Orders::CUSTKEY});

            ColFilter orderCommentFilter({new SimplePredicate(Orders::COMMENT, [](const DataField &input) {
                ByteArray &value = input.asByteArray();
                const char *begin = (const char *) value.ptr;
                char *index = lqf::util::strnstr(begin, "special", value.len);
                if (index != NULL) {
                    return NULL == lqf::util::strnstr(index, "requests", value.len - (index - begin) - 7);
                }
                return true;
            })});
            auto validOrder = orderCommentFilter.filter(*order);

            HashAgg orderCustAgg(vector<uint32_t>({1, 1}), {AGI(Orders::CUSTKEY)},
                                 []() { return vector<AggField *>{new agg::Count()}; }, COL_HASHER(Orders::CUSTKEY));
            // CUSTKEY, COUNT
            auto orderCount = orderCustAgg.agg(*validOrder);

            class CustCountBuilder : public RowBuilder {
            public:
                CustCountBuilder() : RowBuilder({JR(1)}, true) {}

                void build(DataRow &target, DataRow &left, DataRow &right, int key) {
                    target[0] = key;
                    if (&right == &MemDataRow::EMPTY) {
                        target[1] = 0;
                    } else {
                        target[1] = right[0];
                    }
                }
            };
            HashJoin join(Customer::CUSTKEY, 0, new CustCountBuilder());
            join.useOuter();

            // CUSTKEY, COUNT
            auto custCount = join.join(*customer, *orderCount);

            HashAgg countAgg(vector<uint32_t>{1, 1}, {AGI(1)}, []() { return vector<AggField *>{new agg::Count()}; },
                             COL_HASHER(1));
            // COUNT, DIST
            auto result = countAgg.agg(*custCount);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SILE(1) || (SIE(1) && SILE(0));
            };

            SmallSort sort(comparator);

            result = sort.sort(*result);

            auto printer = Printer::Make(PBEGIN PI(0) PI(1) PEND);
            printer->
                    print(*result);
        }
    }
}