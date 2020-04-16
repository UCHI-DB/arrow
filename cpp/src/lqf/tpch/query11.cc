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

        namespace q11 {

            ByteArray nationChosen("GERMANY");
            double fraction = 0.0001;

            class CostField : public agg::DoubleSum {
            public:
                CostField() : agg::DoubleSum(0) {}

                void reduce(DataRow &row) {
                    *value_ += row[PartSupp::AVAILQTY].asInt() * row[PartSupp::SUPPLYCOST].asDouble();
                }
            };
        }
        using namespace q11;

        void executeQ11() {

            auto nation = ParquetTable::Open(Nation::path, {Nation::NATIONKEY, Nation::NAME});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::NATIONKEY, Supplier::SUPPKEY});
            auto partsupp = ParquetTable::Open(PartSupp::path,
                                               {PartSupp::SUPPKEY, PartSupp::PARTKEY, PartSupp::AVAILQTY,
                                                PartSupp::SUPPLYCOST});

            ColFilter nationNameFilter({new SimplePredicate(Nation::NAME, [=](const DataField &field) {
                return field.asByteArray() == nationChosen;
            })});
            auto validNation = nationNameFilter.filter(*nation);

            HashFilterJoin validSupplierJoin(Supplier::NATIONKEY, Nation::NATIONKEY);
            auto validSupplier = validSupplierJoin.join(*supplier, *validNation);

            HashFilterJoin validPsJoin(PartSupp::SUPPKEY, Supplier::SUPPKEY);
            auto validps = FilterMat().mat(*validPsJoin.join(*partsupp, *validSupplier));

            function<vector<AggField *>()> agg_fields = []() { return vector<AggField *>{new CostField()}; };
            SimpleAgg totalAgg(vector<uint32_t>({1}), agg_fields);
            auto total = totalAgg.agg(*validps);
            double total_value = (*(*total->blocks()->collect())[0]->rows())[0][0].asDouble();
            double threshold = total_value * fraction;

            HashAgg bypartAgg(vector<uint32_t>({1, 1}), {AGI(PartSupp::PARTKEY)}, agg_fields,
                              COL_HASHER(PartSupp::PARTKEY));
            bypartAgg.setPredicate([=](DataRow &input) {
                return input[1].asDouble() >= threshold;
            });
            auto byParts = bypartAgg.agg(*validps);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) { return SDGE(1); };
            SmallSort sort(comparator);
            auto sorted = sort.sort(*byParts);

            Printer printer(PBEGIN PI(0) PD(1) PEND);
            printer.print(*sorted);
        }

    }
}