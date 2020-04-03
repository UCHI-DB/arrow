//
// Created by harper on 2/25/20.
//

#include "join.h"

using namespace std;
using namespace std::placeholders;

namespace lqf {
    namespace join {

        JoinBuilder::JoinBuilder(initializer_list<int32_t> initList, bool needkey, bool vertical) :
                needkey_(needkey), vertical_(vertical) {
            init(initList);
        }

        void JoinBuilder::init(initializer_list<int32_t> fields) {
            num_fields_ = fields.size() + needkey_;
            num_fields_string_ = 0;
            uint32_t i = needkey_;

            auto right_num = 0u;
            auto right_nfs = 0u;

            for (auto &inst: fields) {
                auto index = inst & 0xffff;
                num_fields_string_ += inst >> 17;
                if (inst & 0x10000) {
                    // Right
                    ++right_num;
                    right_inst_.push_back(pair<uint8_t, uint8_t>(index, i));
                    right_nfs += inst >> 17;
                } else {
                    left_inst_.push_back(pair<uint8_t, uint8_t>(index, i));
                }
                ++i;
            }

            if (right_nfs == 0) {
                right_col_offsets_ = colOffset(right_num);
                right_col_size_ = colSize(right_num);
            } else {
                uint32_t offset = 0;
                right_col_offsets_.push_back(offset);
                for (auto idx = 0u; idx < right_num - right_nfs; ++idx) {
                    offset += 1;
                    right_col_offsets_.push_back(offset);
                    right_col_size_.push_back(1);
                }
                for (auto idx = 0u; idx < right_nfs; ++idx) {
                    offset += 2;
                    right_col_offsets_.push_back(offset);
                    right_col_size_.push_back(2);
                    offset += 2;
                }
            }
        }

        shared_ptr<MemDataRow> JoinBuilder::snapshot(DataRow &input) {
            auto res = make_shared<MemDataRow>(right_col_offsets_);
            for (uint32_t i = 0; i < right_inst_.size(); ++i) {
                (*res)[i] = input[right_inst_[i].first];
            }
            return res;
        }

        RowBuilder::RowBuilder(initializer_list<int32_t> fields, bool needkey)
                : JoinBuilder(fields, needkey, false) {}

        void RowBuilder::build(DataRow &output, DataRow &left, DataRow &right, int key) {
            uint32_t rsize = right_inst_.size();
            if (needkey_) {
                output[0] = key;
            }
            for (auto &litem : left_inst_) {
                output[litem.second] = left[litem.first];
            }
            for (uint32_t i = 0; i < rsize; ++i) {
                output[right_inst_[i].second] = right[i];
            }
        }

        ColumnBuilder::ColumnBuilder(initializer_list<int32_t> fields)
                : JoinBuilder(fields, false, true) {}

        HashPredicate::HashPredicate() {}

        void HashPredicate::add(uint32_t val) {
            content_.insert(val);
        }

        bool HashPredicate::test(uint32_t val) {
            return content_.find(val) != content_.end();
        }

        BitmapPredicate::BitmapPredicate(uint32_t size) : bitmap_(size) {}

        void BitmapPredicate::add(uint32_t val) {
            bitmap_.put(val);
        }

        bool BitmapPredicate::test(uint32_t val) {
            return bitmap_.check(val);
        }

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

        unique_ptr<IntPredicate> HashBuilder::buildHashPredicate(Table &input, uint32_t keyIndex) {
            auto predicate = new HashPredicate();

            function<void(const shared_ptr<Block> &)> processor = [=](const shared_ptr<Block> &block) {
                auto col = block->col(keyIndex);
                for (uint32_t i = 0; i < block->size(); ++i) {
                    auto key = col->next().asInt();
                    predicate->add(key);
                }
            };
            input.blocks()->foreach(processor);
            return unique_ptr<IntPredicate>(predicate);
        }

        unique_ptr<IntPredicate> HashBuilder::buildBitmapPredicate(Table &input, uint32_t keyIndex) {
            ParquetTable &ptable = (ParquetTable &) input;

            auto predicate = new BitmapPredicate(ptable.size());
            function<void(const shared_ptr<Block> &)> processor = [=](const shared_ptr<Block> &block) {
                auto col = block->col(keyIndex);
                for (uint32_t i = 0; i < block->size(); ++i) {
                    auto key = col->next().asInt();
                    predicate->add(key);
                }
            };
#ifdef LQF_PARALLEL
            input.blocks()->parallel()->foreach(processor);
#else
            input.blocks()->foreach(processor);
#endif
            return unique_ptr<IntPredicate>(predicate);
        }

        unique_ptr<HashContainer> HashBuilder::buildContainer(Table &input, uint32_t keyIndex,
                                                              JoinBuilder *builder) {
            auto container = new HashContainer();
            function<void(const shared_ptr<Block> &)> processor = [builder, keyIndex, container](
                    const shared_ptr<Block> &block) {
                auto rows = block->rows();
                for (uint32_t i = 0; i < block->size(); ++i) {
                    auto key = rows->next()[keyIndex].asInt();
                    auto snap = builder->snapshot((*rows)[i]);
                    container->add(key, snap);
                }
            };
            input.blocks()->foreach(processor);
            return unique_ptr<HashContainer>(container);
        }
    }

    using namespace join;

    HashBasedJoin::HashBasedJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *builder)
            : leftKeyIndex_(leftKeyIndex), rightKeyIndex_(rightKeyIndex), builder_(unique_ptr<JoinBuilder>(builder)),
              container_() {}


    shared_ptr<Table> HashBasedJoin::join(Table &left, Table &right) {
        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, builder_.get());
        auto memTable = MemTable::Make(builder_->numFields(), builder_->numStringFields(),
                                       builder_->useVertical());
        function<void(const shared_ptr<Block> &)> prober = bind(&HashBasedJoin::probe, this, memTable.get(), _1);
        left.blocks()->foreach(prober);
        return memTable;
    }

    HashJoin::HashJoin(uint32_t lk, uint32_t rk, lqf::RowBuilder *builder,
                       function<bool(DataRow &, DataRow &)> pred) : HashBasedJoin(lk, rk, builder),
                                                                    rowBuilder_(builder), predicate_(pred) {}

    void HashJoin::probe(MemTable *output, const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();
        auto resultblock = output->allocate(leftBlock->size());
        uint32_t counter = 0;
        auto writer = resultblock->rows();
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            DataField &key = leftkeys->next();
            auto leftval = key.asInt();
            auto result = container_->get(leftval);
            if (result) {
                DataRow &row = (*leftrows)[leftkeys->pos()];
                if (predicate_(row, *result)) {
                    rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                }
            }
        }
        resultblock->compact(counter);
    }

    HashFilterJoin::HashFilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex) : leftKeyIndex_(leftKeyIndex),
                                                                                    rightKeyIndex_(rightKeyIndex) {}


    shared_ptr<Table> HashFilterJoin::join(Table &left, Table &right) {
        predicate_ = HashBuilder::buildHashPredicate(right, rightKeyIndex_);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashFilterJoin::probe, this, _1);
        return make_shared<TableView>(left.numFields(), left.blocks()->map(prober));
    }


    shared_ptr<Block> HashFilterJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto col = leftBlock->col(leftKeyIndex_);
        auto bitmap = make_shared<SimpleBitmap>(leftBlock->limit());
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            auto key = col->next().asInt();
            if (predicate_->test(key)) {
                bitmap->put(col->pos());
            }
        }
        return leftBlock->mask(bitmap);
    }

    HashExistJoin::HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                                 RowBuilder *builder,
                                 function<bool(DataRow &, DataRow &)> pred)
            : HashJoin(leftKeyIndex, rightKeyIndex, builder, pred) {}

    void HashExistJoin::probe(MemTable *output, const shared_ptr<Block> &leftBlock) {
        auto resultblock = output->allocate(container_->size());

        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        uint32_t counter = 0;
        auto writer = resultblock->rows();
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            DataField &keyfield = leftkeys->next();
            auto key = keyfield.asInt();
            auto result = container_->get(key);
            if (result) {
                DataRow &row = (*leftrows)[leftkeys->pos()];
                if (predicate_(row, *result)) {
                    auto exist = container_->remove(key);
                    (*writer)[counter++] = (*exist);
                }
            }
        }

        resultblock->compact(counter);
    }

    HashColumnJoin::HashColumnJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, lqf::ColumnBuilder *builder)
            : HashBasedJoin(leftKeyIndex, rightKeyIndex, builder), columnBuilder_(builder) {}


    void HashColumnJoin::probe(lqf::MemTable *owner, const shared_ptr<Block> &leftBlock) {
        shared_ptr<MemvBlock> leftvBlock = static_pointer_cast<MemvBlock>(leftBlock);
        /// Make sure the cast is valid
        assert(leftvBlock.get() != nullptr);

        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        MemvBlock vblock(leftBlock->size(), columnBuilder_->rightColSize());
        auto writer = vblock.rows();
        for (uint32_t i = 0; i < leftBlock->size(); ++i) {
            DataField &key = leftkeys->next();
            auto leftval = key.asInt();
            auto result = container_->get(leftval);
            (*writer)[i] = *result;
        }

        auto newblock = owner->allocate(0);
        // Merge result block with original block
        auto newvblock = static_pointer_cast<MemvBlock>(newblock);

        newvblock->merge(*leftvBlock, columnBuilder_->leftInst());
        newvblock->merge(vblock, columnBuilder_->rightInst());
    }
}
