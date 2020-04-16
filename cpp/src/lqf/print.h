//
// Created by harper on 2/27/20.
//

#ifndef ARROW_PRINT_H
#define ARROW_PRINT_H

#include <functional>
#include <string>
#include <iomanip>
#include "data_model.h"

#define PBEGIN [=](DataRow& row) { std::cout << std::setprecision(2) << std::fixed <<
#define PEND std::endl ;}
#define PI(x) row[x].asInt() << ",\t" <<
#define PD(x) row[x].asDouble() << ",\t" <<
#define PB(x) row[x].asByteArray() << ",\t" <<
#define PR(x) row(x).asInt() << "," <<
#define PDICT(dict, x) (*dict)[row[x].asInt()] << ",\t" <<

using namespace std;
using namespace std::placeholders;
namespace lqf {

    ostream &operator<<(ostream &os, const ByteArray &dt);

    class Printer {
    protected:
        uint64_t sum_;

        function<void(DataRow & )> linePrinter_;

        virtual void printBlock(const shared_ptr<Block> &block);

    public:
        Printer(function<void(DataRow & )> linePrinter);

        void print(Table &table);

        static unique_ptr<Printer> Make(function<void(DataRow & )> linePrinter);
    };
}
#endif //ARROW_PRINT_H
