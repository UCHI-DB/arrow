//
// Created by harper on 2/25/20.
//

#include "join.h"

using namespace std;
using namespace std::placeholders;

namespace lqf {


    RowBuilder::RowBuilder(initializer_list<uint32_t> left, initializer_list<uint32_t> right, bool needkey)
            : left_(left), right_(right), needkey_(needkey) {};

    shared_ptr<MemDataRow> RowBuilder::snapshot(DataRow &input) {
        auto res = make_shared<MemDataRow>(right_.size());
        for (uint32_t i = 0; i < right_.size(); ++i) {
            (*res)[i] = input[right_[i]];
        }
        return res;
    };

    uint32_t RowBuilder::hashSize() {
        return right_.size();
    }

    uint32_t RowBuilder::outputSize() {
        return left_.size() + right_.size();
    };

    void RowBuilder::build(DataRow &output, DataRow &left, DataRow &right, int key) {
        uint32_t lsize = left_.size();
        uint32_t rsize = right_.size();
        for (uint32_t i = 0; i < lsize; ++i) {
            output[i] = left[left_[i]];
        }
        for (uint32_t i = 0; i < rsize; ++i) {
            output[i + lsize] = right[i];
        }
        if (needkey_) {
            output[lsize + rsize] = key;
        }
    };

    HashContainer::HashContainer() : hashmap_() {}

    void HashContainer::add(int32_t key, shared_ptr<DataRow> dataRow) {
        min_ = min(min_, key);
        max_ = max(max_, key);
        hashmap_[key] = dataRow;
    }

    shared_ptr<DataRow> HashContainer::get(int32_t key) {
        if (key > max_ || key < min_)
            return nullptr;
        auto it = hashmap_.find(key);
        if (it != hashmap_.end()) {
            return it->second;
        }
        return nullptr;
    }

    shared_ptr<DataRow> HashContainer::remove(int32_t key) {
        if (key > max_ || key < min_)
            return nullptr;
        auto it = hashmap_.find(key);
        if (it != hashmap_.end()) {
            auto value = it->second;
            hashmap_.erase(it);
            return value;
        }
        return nullptr;
    }

    uint32_t HashContainer::size() {
        return hashmap_.size();
    }

    HashJoin::HashJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, RowBuilder *builder,
                       function<bool(DataRow &, DataRow &)> pred) : leftKeyIndex_(leftKeyIndex),
                                                                    rightKeyIndex_(rightKeyIndex), predicate_(pred),
                                                                    rowBuilder_(unique_ptr<RowBuilder>(builder)),
                                                                    container_() {}


    shared_ptr<Table> HashJoin::join(Table &left, Table &right) {
        function<void(const shared_ptr<Block> &)> buildHash = bind(&HashJoin::build, this, _1);
        right.blocks()->foreach(buildHash);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashJoin::probe, this, _1);
        return make_shared<TableView>(rowBuilder_->outputSize(), left.blocks()->map(prober));
    }

    void HashJoin::build(const shared_ptr<Block> &rightBlock) {
        auto rows = rightBlock->rows();
        for (uint32_t i = 0; i < rightBlock->size(); ++i) {
            auto key = rows->next()[rightKeyIndex_].asInt();
            auto snap = rowBuilder_->snapshot((*rows)[i]);
            container_.add(key, snap);
        }
    }

    shared_ptr<Block> HashJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();
        auto resultblock = make_shared<MemBlock>(leftBlock->size(), rowBuilder_->outputSize());
        uint32_t counter = 0;
        auto writer = resultblock->rows();
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            DataField &key = leftkeys->next();
            auto leftval = key.asInt();
            auto result = container_.get(leftval);
            if (result) {
                DataRow &row = (*leftrows)[leftkeys->pos()];
                if (predicate_(row, *result)) {
                    rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                }
            }
        }

        resultblock->compact(counter);
        return resultblock;
    }

    HashFilterJoin::HashFilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex) : leftKeyIndex_(leftKeyIndex),
                                                                                    rightKeyIndex_(rightKeyIndex) {}


    shared_ptr<Table> HashFilterJoin::join(Table &left, Table &right) {
        function<void(const shared_ptr<Block> &)> buildHash = bind(&HashFilterJoin::build, this, _1);
        right.blocks()->foreach(buildHash);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashFilterJoin::probe, this, _1);
        return make_shared<TableView>(left.numFields(), left.blocks()->map(prober));
    }

    void HashFilterJoin::build(const shared_ptr<Block> &rightBlock) {
        auto rows = rightBlock->rows();
        for (uint32_t i = 0; i < rightBlock->size(); ++i) {
            auto key = rows->next()[rightKeyIndex_].asInt();
            container_.add(key, nullptr);
        }
    }

    shared_ptr<Block> HashFilterJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto col = leftBlock->col(leftKeyIndex_);
        auto bitmap = make_shared<SimpleBitmap>(leftBlock->size());
        uint32_t counter = 0;
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            auto key = col->next().asInt();
            if (container_.get(key)) {
                bitmap->put(col->pos());
            }
        }
        return leftBlock->mask(bitmap);
    }

    HashExistJoin::HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                                 RowBuilder *builder,
                                 function<bool(DataRow &, DataRow &)> pred)
            : HashJoin(leftKeyIndex, rightKeyIndex, move(builder), pred) {}

    shared_ptr<Block> HashExistJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();
        auto resultblock = make_shared<MemBlock>(this->container_.size(), rowBuilder_->hashSize());
        uint32_t counter = 0;
        auto writer = resultblock->rows();
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            DataField &keyfield = leftkeys->next();
            auto key = keyfield.asInt();
            auto result = container_.get(key);
            if (result) {
                DataRow &row = (*leftrows)[leftkeys->pos()];
                if (predicate_(row, *result)) {
                    auto exist = container_.remove(key);
                    static_cast<MemDataRow &>((*writer)[counter++]) = static_cast<MemDataRow &> (*exist);
                }
            }
        }

        resultblock->compact(counter);
        return resultblock;
    }
}
