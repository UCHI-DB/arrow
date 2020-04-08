//
// Created by harper on 4/8/20.
//

#include "union.h"

namespace lqf {

    shared_ptr<Table> FilterUnion::execute(initializer_list<Table *> tables) {

        unordered_map<uint32_t, shared_ptr<Bitmap>> map;
        mutex write_lock;
        ParquetTable *owner;
        function<void(const shared_ptr<Block> &)> processor = [&map, &owner, &write_lock](
                const shared_ptr<Block> &block) {
            auto mblock = dynamic_pointer_cast<MaskedBlock>(block);
            owner = static_cast<ParquetTable *>(mblock->inner()->owner());
            auto blockid = mblock->inner()->id();

            write_lock.lock();
            if (map.find(blockid) != map.end()) {
                (*map[blockid]) | (*mblock->mask());
            } else {
                map[blockid] = mblock->mask();
            }
            write_lock.unlock();
        };
        for (auto &table: tables) {
            table->blocks()->foreach(processor);
        }

        return make_shared<MaskedTable>(owner, map);
    }
}