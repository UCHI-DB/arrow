//
// Created by harper on 3/3/20.
//

#ifndef ARROW_TPCH_QUERY_H
#define ARROW_TPCH_QUERY_H

#include <string>

using namespace std;

namespace lqf {
    namespace tpch {


        static const string tablePath(const string &name) {
            std::ostringstream stringStream;
            stringStream << "/local/hajiang/tpch/5/presto/" << name << "/" << name << ".parquet";
            return stringStream.str();
        }

        class LineItem {
        public:
            static const string path;
            static const int ORDERKEY = 0;
            static const int PARTKEY = 1;
            static const int SUPPKEY = 2;
            static const int LINENUMBER = 3;
            static const int QUANTITY = 4;
            static const int EXTENDEDPRICE = 5;
            static const int DISCOUNT = 6;
            static const int TAX = 7;
            static const int RETURNFLAG = 8;
            static const int LINESTATUS = 9;
            static const int SHIPDATE = 10;
            static const int COMMITDATE = 11;
            static const int RECEIPTDATE = 12;
            static const int SHIPINSTRUCT = 13;
            static const int SHIPMODE = 14;
            static const int COMMENT = 15;
        };

        const string LineItem::path = tablePath("lineitem");

        class Part {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int NAME = 1;
            static const int MFGR = 2;
            static const int BRAND = 3;
            static const int TYPE = 4;
            static const int SIZE = 5;
            static const int CONTAINER = 6;
            static const int RETAILPRICE = 7;
            static const int COMMENT = 8;
        };

        const string Part::path = tablePath("part");

        class PartSupp {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int SUPPKEY = 1;
            static const int AVAILQTY = 2;
            static const int SUPPLYCOST = 3;
            static const int COMMENT = 4;
        };

        const string PartSupp::path = tablePath("partsupp");

        class Supplier {
        public:
            static const string path;
            static const int SUPPKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int NATIONKEY = 3;
            static const int PHONE = 4;
            static const int ACCTBAL = 5;
            static const int COMMENT = 6;
        };

        const string Supplier::path = tablePath("supplier");

        class Nation {
        public:
            static const string path;
            static const int NATIONKEY = 0;
            static const int NAME = 1;
            static const int REGIONKEY = 2;
            static const int COMMENT = 3;
        };

        const string Nation::path = tablePath("nation");

        class Region {
        public:
            static const string path;
            static const int REGIONKEY = 0;
            static const int NAME = 1;
            static const int COMMENT = 2;
        };

        const string Region::path = tablePath("region");
    }
}
#endif //ARROW_TPCH_QUERY_H
