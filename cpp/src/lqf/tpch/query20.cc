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
        namespace q20 {
            ByteArray dateFrom("1994-01-01");
            ByteArray dateTo("1995-01-01");


        }
        using namespace q20;

        void executeQ20() {
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY, Supplier::NAME,
                                                                Supplier::ADDRESS});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::QUANTITY, LineItem::PARTKEY, LineItem::SUPPKEY,
                                                LineItem::SHIPDATE});
            auto part = ParquetTable::Open(Part::path, {Part::NAME, Part::PARTKEY});
            auto partsupp = ParquetTable::Open(PartSupp::path, {PartSupp::PARTKEY, PartSupp::SUPPKEY});

            using namespace sboost;
            ColFilter suppFilter(new SboostPredicate<Int32Type>(Supplier::NATIONKEY,
                                                                bind(&Int32DictEq::build, 3)));
            auto validSupplier = FilterMat().mat(*suppFilter.filter(*supplier));

            ColFilter partFilter(
                    new SimplePredicate(Part::NAME, [](const DataField &field) {
                        ByteArray &val = field.asByteArray();
                        char *start = (char *) val.ptr;
                        return lqf::util::strnstr(start, "forest", val.len) == start;
                    }));
            auto validpart = partFilter.filter(*part);

            HashFilterJoin partfilterPs(PartSupp::PARTKEY, Part::PARTKEY);
            auto validps = partfilterPs.join(*partsupp, *validpart);

            HashFilterJoin suppFilterPs(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            validps = suppFilterPs.join(*validps, *validSupplier);

            function < int64_t(DataRow & ) > key_ext = [](DataRow &input) {
                return (static_cast<int64_t>(input[0].asInt()) << 32) +
                       input[1].asInt();
            };

            FilterMat psMat;
            validps = psMat.mat(*validps);

            ColFilter lineitemFilter(new SboostPredicate<ByteArrayType>(LineItem::SHIPDATE,
                                                                        bind(ByteArrayDictRangele::build, dateFrom,
                                                                             dateTo)));
            auto validLineitem = lineitemFilter.filter(*lineitem);

            using namespace powerjoin;
            function < int64_t(DataRow & ) > key_ext_1 = [](DataRow &input) {
                return (static_cast<int64_t>(input[LineItem::PARTKEY].asInt()) << 32) +
                       input[LineItem::SUPPKEY].asInt();
            };

            PowerHashFilterJoin psFilterItem(key_ext_1, key_ext);
            validLineitem = psFilterItem.join(*validLineitem, *validps);

            using namespace agg;
            HashAgg lineitemQuanAgg(vector < uint32_t > {1, 1, 1}, {AGI(LineItem::PARTKEY), AGI(LineItem::SUPPKEY)},
                                    []() { return vector < AggField * > {new IntSum(LineItem::QUANTITY)}; },
                                    COL_HASHER2(LineItem::PARTKEY, LineItem::SUPPKEY));
            // PARTKEY SUPPKEY SUM(QUANTITY)
            auto agglineitem = lineitemQuanAgg.agg(*validLineitem);

            PowerHashJoin psFilterItem2(key_ext, key_ext, new RowBuilder({JL(1)}, false),
                                        [](DataRow &left, DataRow &right) {
                                            return left[2].asInt() > right[2].asInt() * 0.5;
                                        });
            // SUPPKEY
            auto suppkeys = psFilterItem2.join(*validps, *agglineitem);

            HashFilterJoin finalFilter(0, 0);
            auto supplierResult = finalFilter.join(*validSupplier, *suppkeys);

            function<bool(DataRow * , DataRow * )> comparator = [](DataRow *a, DataRow *b) {
                return SBLE(0);
            };
            vector <uint32_t> snapshot{0, 2, 4};

            SmallSort sort(comparator);
            supplierResult = sort.sort(*supplierResult);

            auto printer = Printer::Make(PBEGIN PB(0) PB(1) PEND);
            printer->print(*supplierResult);
        }
    }
}