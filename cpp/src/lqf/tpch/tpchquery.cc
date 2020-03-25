//
// Created by harper on 3/25/20.
//
#include "tpchquery.h"

namespace lqf {
    namespace tpch {

        static const string tablePath(const string &name) {
            std::ostringstream stringStream;
            stringStream << "/local/hajiang/tpch/5/presto/" << name << "/" << name << ".parquet";
            return stringStream.str();
        }

        const string LineItem::path = tablePath("lineitem");

        const string Part::path = tablePath("part");

        const string PartSupp::path = tablePath("partsupp");

        const string Supplier::path = tablePath("supplier");

        const string Nation::path = tablePath("nation");

        const string Region::path = tablePath("region");
    }
}