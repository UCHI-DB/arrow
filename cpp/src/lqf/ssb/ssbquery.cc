//
// Created by harper on 3/25/20.
//
#include <parquet/types.h>
#include "ssbquery.h"

namespace lqf {
    namespace ssb {

        static const string tablePath(const string &name) {
            std::ostringstream stringStream;
            stringStream << "/local/hajiang/ssb/1/" << name << ".parquet";
            return stringStream.str();
        }

        const string LineOrder::path = tablePath("lineorder");

        const string Part::path = tablePath("part");

        const string Supplier::path = tablePath("supplier");

        const string Customer::path = tablePath("customer");

    }

    namespace udf {
        int date2year(parquet::ByteArray &date) {
            return (date.ptr[0] - '0') * 1000 + (date.ptr[1] - '0') * 100 + (date.ptr[2] - '0') * 10 + date.ptr[3] -
                   '0';
        }
    }
}