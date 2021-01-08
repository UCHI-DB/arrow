//
// Created by harper on 3/3/20.
//

#ifndef ARROW_SSBQUERY_H
#define ARROW_SSBQUERY_H

#include <string>
#include <sstream>
#include <parquet/types.h>

using namespace std;

namespace lqf {
    namespace ssb {

        class LineOrder {
        public:
            static const string path;
            static const int ORDERKEY = 0;
            static const int LINENUMBER = 1;
            static const int CUSTKEY = 2;
            static const int PARTKEY = 3;
            static const int SUPPKEY = 4;
            static const int ORDERDATE = 5;
            static const int ORDPRIORITY = 6;
            static const int SHIPPRIORITY = 7;
            static const int QUANTITY = 8;
            static const int EXTENDEDPRICE = 9;
            static const int ORDTOTALPRICE = 10;
            static const int DISCOUNT = 11;
            static const int REVENUE = 12;
            static const int SUPPLYCOST = 13;
            static const int TAX = 14;
            static const int COMMITDATE = 15;
            static const int SHIPMODE = 16;
        };

        class Part {
        public:
            static const string path;
            static const int PARTKEY = 0;
            static const int NAME = 1;
            static const int MFGR = 2;
            static const int CATEGORY = 3;
            static const int BRAND = 4;
            static const int COLOR = 5;
            static const int TYPE = 6;
            static const int SIZE = 7;
            static const int CONTAINER = 8;
        };

        class Supplier {
        public:
            static const string path;
            static const int SUPPKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int CITY = 3;
            static const int NATION = 4;
            static const int REGION = 5;
            static const int PHONE = 6;
        };

        class Customer {
        public:
            static const string path;
            static const int CUSTKEY = 0;
            static const int NAME = 1;
            static const int ADDRESS = 2;
            static const int CITY = 3;
            static const int NATION = 4;
            static const int REGION = 5;
            static const int PHONE = 6;
            static const int MKTSEGMENT = 7;
        };

        class Date {
        public:
            static const string path;
            static const int DATEKEY = 0;
            static const int DATE = 1;
            static const int DAYOFWEEK = 2;
            static const int MONTH = 3;
            static const int YEAR = 4;
            static const int YEARMONTHNUM = 5;
            static const int YEARMONTH = 6;
            static const int DAYNUMINWEEK = 7;
            static const int DAYNUMINMONTH = 8;
            static const int DAYNUMINYEAR = 9;
            static const int MONTHNUMINYEAR = 10;
            static const int WEEKNUMINYEAR = 11;
            static const int SELLINGSEASON = 12;
            static const int LASTDAYINWEEKFL = 13;
            static const int LASTDAYINMONTHFL = 14;
            static const int HOLIDAYFL = 15;
            static const int WEEKDAYFL = 16;
        };

        void executeQ1_1();
        void executeQ1_2();
        void executeQ1_3();
        void executeQ2_1();
        void executeQ2_2();
        void executeQ2_3();
        void executeQ3_1();
        void executeQ3_2();
        void executeQ3_3();
        void executeQ4_1();
        void executeQ4_2();
        void executeQ4_3();

        namespace udf {
            int date2year(parquet::ByteArray &);
        }
    }


}
#endif //ARROW_SSBQUERY_H
