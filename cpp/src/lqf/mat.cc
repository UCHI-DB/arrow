//
// Created by harper on 3/17/20.
//

#include "mat.h"

using namespace std::placeholders;

namespace lqf {

    MemMat::MemMat(uint32_t num_fields, function<void(DataRow &, DataRow &)> loader)
            : num_fields_(num_fields), loader_(loader) {}

    shared_ptr<MemTable> MemMat::mat(Table &input) {
        auto mtable = MemTable::Make(num_fields_);

        function<void(const shared_ptr<Block> &)> mapper = bind(&MemMat::matBlock, this, mtable.get(), _1);
        input.blocks()->foreach(mapper);
        return mtable;
    }

    void MemMat::matBlock(MemTable *table, const shared_ptr<Block> &block) {
        auto mblock = table->allocate(block->size());

        auto irows = block->rows();
        auto orows = mblock->rows();

        auto size = block->size();
        for (uint32_t i = 0; i < size; ++i) {
            loader_((*irows)[i], (*orows)[i]);
        }
    }
}