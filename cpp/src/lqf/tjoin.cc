//
// Created by harper on 1/24/21.
//

#include "tjoin.h"

namespace lqf {
    template<typename Container>
    HashBasedTJoin<Container>::HashBasedTJoin(uint32_t lki, uint32_t rki, JoinBuilder *builder,
                                 uint32_t expect_size)
            : left_key_index_(lki), right_key_index_(rki),
              builder_(unique_ptr<JoinBuilder>(builder)), expect_size_(expect_size) {}

    template<typename Container>
    shared_ptr<Table> HashBasedTJoin<Container>::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();

        container_ = Container::build(right, rightKeyIndex_, builder_->snapshoter(), expect_size_);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashBasedTJoin<Container>::probe, this, _1);
        return makeTable(left.blocks()->map(prober));
    }

    template<typename Container>
    shared_ptr<Block> HashBasedTJoin<Container>::makeBlock(uint32_t size) {
        if (builder_->useVertical())
            return make_shared<MemvBlock>(size, builder_->outputColSize());
        else
            return make_shared<MemBlock>(size, builder_->outputColOffset());
    }

    template<typename Container>
    shared_ptr<TableView> HashBasedTJoin<Container>::makeTable(unique_ptr<Stream<shared_ptr<Block>>> stream) {
        return make_shared<TableView>(builder_->useVertical() ? OTHER : RAW, builder_->outputColSize(), move(stream));
    }
}