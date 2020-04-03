//
// Created by harper on 2/27/20.
//

#ifndef ARROW_PRINT_H
#define ARROW_PRINT_H

#include <functional>
#include <string>
#include "data_model.h"

#define PBEGIN [](DataRow& row) { std::cout <<
#define PEND std::endl ;}
#define PI(x) row[x].asInt() << ", " <<
#define PD(x) row[x].asDouble() << ", " <<
#define PB(x) row[x].asByteArray() << ", " <<

using namespace std;
using namespace std::placeholders;
namespace lqf {

    class Printer {
    protected:
        uint64_t sum_;

        function<void(DataRow &)> linePrinter_;

        void printBlock(const shared_ptr<Block> &block);

    public:
        static unique_ptr<Printer> Make(function<void(DataRow &)> linePrinter);

        void print(Table &table);
    };

    ostream &operator<<(ostream &os, const ByteArray &dt);
}
#endif //ARROW_PRINT_H
