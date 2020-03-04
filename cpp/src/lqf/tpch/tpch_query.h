//
// Created by harper on 3/3/20.
//

#ifndef ARROW_TPCH_QUERY_H
#define ARROW_TPCH_QUERY_H

#include <string>

using namespace std;

namespace lqf {
    namespace tpch {

        class Config {
        public:
            static const string &tablePath(const string &name) {
                return name;
            }
        };

        class LineItem {
        public:
            static const string &path;
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

        const string &LineItem::path = Config::tablePath("lineitem");
    }
}
#endif //ARROW_TPCH_QUERY_H
