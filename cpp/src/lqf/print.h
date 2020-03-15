//
// Created by harper on 2/27/20.
//

#ifndef ARROW_PRINT_H
#define ARROW_PRINT_H

#include <functional>
#include "data_model.h"

#define PBEGIN [](DataRow& row) { std::cout <<
#define PEND std::endl ;}
#define PINT(x) row[x].asInt() << ", " <<
#define PDOUBLE(x) row[x].asDouble() << ", " <<
#define PSTRING " " <<

using namespace std;
using namespace std::placeholders;
namespace lqf {

    class Printer {
    protected:
        function<void(DataRow &)> linePrinter_;

        void printBlock(const shared_ptr<Block> &block);

    public:
        static unique_ptr<Printer> Make(function<void(DataRow &)> linePrinter);

        void print(Table &table);
    };
}
#endif //ARROW_PRINT_H
