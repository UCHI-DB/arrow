//
// Created by harper on 3/17/20.
//

#include "mat.h"
#include "join.h"

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
            loader_((*irows).next(), (*orows)[i]);
        }
    }

    shared_ptr<Table> FilterMat::mat(Table &input) {
        unordered_map<uint32_t, shared_ptr<Bitmap>> map;
        ParquetTable *owner;
        function<void(const shared_ptr<Block> &)> processor = [&map, &owner](const shared_ptr<Block> &block) {
            auto mblock = dynamic_pointer_cast<MaskedBlock>(block);
            owner = static_cast<ParquetTable *>(mblock->inner()->owner());
            map[mblock->inner()->id()] = mblock->mask();
        };
        input.blocks()->foreach(processor);

        return make_shared<MaskedTable>(owner, map);
    }

    HashMat::HashMat(uint32_t key_index, function<unique_ptr<MemDataRow>(DataRow &)> snapshoter) :
            key_index_(key_index), snapshoter_(snapshoter) {}

    shared_ptr<Table> HashMat::mat(Table &input) {
        auto table = MemTable::Make(0);
        if (snapshoter_) {
            // Make Container
            auto container = HashBuilder::buildContainer(input, key_index_, snapshoter_);
            auto block = make_shared<HashMemBlock>(move(container));
            table->append(block);
        } else {
            auto predicate = HashBuilder::buildHashPredicate(input, key_index_);
            auto block = make_shared<HashMemBlock>(move(predicate));
            table->append(block);
        }
        return table;
    }

    PowerHashMat::PowerHashMat(function<int64_t(DataRow &)> key_maker,
                               function<unique_ptr<MemDataRow>(DataRow &)> snapshoter)
            : key_maker_(key_maker), snapshoter_(snapshoter) {}

    shared_ptr<Table> PowerHashMat::mat(Table &input) {
        auto table = MemTable::Make(0);
        if (snapshoter_) {
            // Make Container
            auto container = HashBuilder::buildContainer(input, key_maker_, snapshoter_);
            auto block = make_shared<HashMemBlock>(move(container));
            table->append(block);
        } else {
            auto predicate = HashBuilder::buildHashPredicate(input, key_maker_);
            auto block = make_shared<HashMemBlock>(move(predicate));
            table->append(block);
        }
        return table;
    }
}