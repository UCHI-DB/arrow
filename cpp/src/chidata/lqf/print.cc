//
// Created by harper on 2/27/20.
//

#include <chidata/lqf/data_model.h>
#include "print.h"

namespace chidata {
    namespace lqf {

        void Printer::printBlock(shared_ptr<Block> &block) {
            auto rows = block->rows();
            for (uint32_t i = 0; i < block->size(); ++i) {
                linePrinter_(rows->next());
            }
        }

        unique_ptr<Printer> Printer::Make(function<void(DataRow &)> linePrinter) {
            Printer *printer = new Printer();
            printer->linePrinter_ = linePrinter;
            return unique_ptr<Printer>(printer);
        }

        void Printer::print(Table &table) {
            function<void(shared_ptr<Block> &)> printer = bind(&Printer::printBlock, this, _1);
            table.blocks()->foreach(printer);
        }

    }
}