//
// Created by harper on 2/27/20.
//

#include "data_model.h"
#include "print.h"

namespace lqf {

    ostream &operator<<(ostream &os, const ByteArray &dt) {
        os << std::string(reinterpret_cast<const char *>(dt.ptr), dt.len);
        return os;
    }

    void Printer::printBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto block_size = block->size();
        for (uint32_t i = 0; i < block_size; ++i) {
            linePrinter_(rows->next());
        }
        sum_ += block->size();
    }

    unique_ptr<Printer> Printer::Make(function<void(DataRow &)> linePrinter) {
        Printer *printer = new Printer();
        printer->linePrinter_ = linePrinter;
        return unique_ptr<Printer>(printer);
    }

    void Printer::print(Table &table) {
        sum_ = 0;
        function<void(const shared_ptr<Block> &)> printer = bind(&Printer::printBlock, this, _1);
        table.blocks()->foreach(printer);
        cout << "Total: " << sum_ << " rows" << endl;
    }

}
