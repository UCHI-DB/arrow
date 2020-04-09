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
            uint32_t i = needkey_;

            output_col_offsets_.push_back(0);
            if (needkey_) {
                output_col_offsets_.push_back(1);
                output_col_size_.push_back(1);
            }
            right_col_offsets_.push_back(0);
            for (auto &inst: fields) {
                auto index = inst & 0xffff;
                bool is_string = inst >> 17;
                bool is_raw = inst >> 18;
                bool is_right = inst & 0x10000;

                output_col_size_.push_back(is_string ? 2 : 1);
                output_col_offsets_.push_back(output_col_offsets_.back() + output_col_size_.back());
                if (is_right) {
                    if (is_raw) {
                        right_raw_.push_back(pair<uint8_t, uint8_t>(index, i));
                    } else {
                        right_inst_.push_back(pair<uint8_t, uint8_t>(index, i));
                    }
                    right_col_size_.push_back(output_col_size_.back());
                    right_col_offsets_.push_back(right_col_offsets_.back() + right_col_size_.back());
                } else {
                    if (is_raw) {
                        left_raw_.push_back(pair<uint8_t, uint8_t>(index, i));
                    } else {
                        left_inst_.push_back(pair<uint8_t, uint8_t>(index, i));
                    }
                }
                ++i;
            }
        }

        unique_ptr<MemDataRow> JoinBuilder::snapshot(DataRow &input) {
            auto res = new MemDataRow(right_col_offsets_);
            uint32_t right_inst_size = right_inst_.size();
            for (uint32_t i = 0; i < right_inst_size; ++i) {
                (*res)[i] = input[right_inst_[i].first];
            }
            uint32_t right_raw_size = right_raw_.size();
            for (uint32_t i = 0; i < right_raw_size; ++i) {
                (*res)[right_inst_size + i] = input(right_raw_[i].first);
            }
            return unique_ptr<MemDataRow>(res);
        }

        RowBuilder::RowBuilder(initializer_list<int32_t> fields, bool needkey, bool vertical)
                : JoinBuilder(fields, needkey, vertical) {}

        void RowBuilder::build(DataRow &output, DataRow &left, DataRow &right, int key) {
            if (needkey_) {
                output[0] = key;
            }
            for (auto &litem : left_inst_) {
                output[litem.second] = left[litem.first];
            }
            for (auto &litem : left_raw_) {
                output[litem.second] = left(litem.first);
            }

            uint32_t rinst_size = right_inst_.size();
            for (uint32_t i = 0; i < rinst_size; ++i) {
                output[right_inst_[i].second] = right[i];
            }
            uint32_t rraw_size = right_raw_.size();
            for (uint32_t i = 0; i < rraw_size; ++i) {
                output[right_raw_[i].second] = right[i + rinst_size];
            }
        }

        ColumnBuilder::ColumnBuilder(initializer_list<int32_t> fields)
                : JoinBuilder(fields, false, true) {}

    }
    namespace hash {
        HashPredicate::HashPredicate() {}

        void HashPredicate::add(int64_t val) {
            content_.insert(val);
        }

        bool HashPredicate::test(int64_t val) {
            return content_.find(val) != content_.end();
        }

        BitmapPredicate::BitmapPredicate(uint64_t size) : bitmap_(size) {}

        void BitmapPredicate::add(int64_t val) {
            bitmap_.put(val);
        }

        bool BitmapPredicate::test(int64_t val) {
            return bitmap_.check(val);
        }

        HashContainer::HashContainer() : hashmap_() {}

        void HashContainer::add(int64_t key, unique_ptr<MemDataRow> dataRow) {
            min_ = min(min_, key);
            max_ = max(max_, key);
            hashmap_[key] = move(dataRow);
        }

        MemDataRow *HashContainer::get(int64_t key) {
            if (key > max_ || key < min_)
                return nullptr;
            auto it = hashmap_.find(key);
            if (it != hashmap_.end()) {
                return it->second.get();
            }
            return nullptr;
        }

        unique_ptr<MemDataRow> HashContainer::remove(int64_t key) {
            if (key > max_ || key < min_)
                return nullptr;
            auto it = hashmap_.find(key);
            if (it != hashmap_.end()) {
                auto value = move(it->second);
                hashmap_.erase(it);
                return value;
            }
            return nullptr;
        }

        uint32_t HashContainer::size() {
            return hashmap_.size();
        }

        HashMemBlock::HashMemBlock(unique_ptr<HashContainer> container)
                : MemBlock(0, 0) {
            container_ = move(container);
        }

        HashMemBlock::HashMemBlock(unique_ptr<IntPredicate> predicate)
                : MemBlock(0, 0) {
            predicate_ = move(predicate);
        }

        unique_ptr<HashContainer> HashMemBlock::container() {
            return move(container_);
        }

        unique_ptr<IntPredicate> HashMemBlock::predicate() {
            return move(predicate_);
        }

        unique_ptr<IntPredicate> HashBuilder::buildHashPredicate(Table &input, uint32_t keyIndex) {
            HashPredicate *predicate = new HashPredicate();

            function<void(const shared_ptr<Block> &)> processor =
                    [&predicate, keyIndex](const shared_ptr<Block> &block) {
                        auto hashblock = dynamic_pointer_cast<HashMemBlock>(block);
                        if (hashblock) {
                            predicate = (HashPredicate *) hashblock->predicate().release();
                            return;
                        }
                        auto col = block->col(keyIndex);
                        for (uint32_t i = 0; i < block->size(); ++i) {
                            auto key = col->next().asInt();
                            predicate->add(key);
                        }
                    };
            input.blocks()->foreach(processor);
            return unique_ptr<IntPredicate>(predicate);
        }

        unique_ptr<IntPredicate> HashBuilder::buildHashPredicate(Table &input, function<int64_t(DataRow &)> key_maker) {
            HashPredicate *predicate = new HashPredicate();

            function<void(const shared_ptr<Block> &)> processor =
                    [&predicate, key_maker](const shared_ptr<Block> &block) {
                        auto hashblock = dynamic_pointer_cast<HashMemBlock>(block);
                        if (hashblock) {
                            predicate = (HashPredicate *) hashblock->predicate().release();
                            return;
                        }
                        auto rows = block->rows();
                        for (uint32_t i = 0; i < block->size(); ++i) {
                            DataRow &row = rows->next();
                            predicate->add(key_maker(row));
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
                                                              function<unique_ptr<MemDataRow>(DataRow &)> builder) {
            HashContainer *container = new HashContainer();
            function<void(const shared_ptr<Block> &)> processor = [builder, keyIndex, &container](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock>(block);
                if (hashblock) {
                    container = (HashContainer *) hashblock->container().release();
                    return;
                }
                auto rows = block->rows();
                for (uint32_t i = 0; i < block->size(); ++i) {
                    DataRow &row = (*rows)[i];
                    auto key = row[keyIndex].asInt();
                    container->add(key, builder(row));
                }
            };
            input.blocks()->foreach(processor);
            return unique_ptr<HashContainer>(container);
        }

        unique_ptr<HashContainer> HashBuilder::buildContainer(Table &input,
                                                              function<int64_t(DataRow &)> key_maker,
                                                              function<unique_ptr<MemDataRow>(DataRow &)> builder) {
            HashContainer *container = new HashContainer();
            function<void(const shared_ptr<Block> &)> processor = [builder, &container, key_maker](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock>(block);
                if (hashblock) {
                    container = (HashContainer *) hashblock->container().release();
                }
                auto rows = block->rows();
                for (uint32_t i = 0; i < block->size(); ++i) {
                    DataRow &row = (*rows)[i];
                    auto key = key_maker(row);
                    container->add(key, builder(row));
                }
            };
            input.blocks()->foreach(processor);
            return unique_ptr<HashContainer>(container);
        }
    }

    using namespace join;
    using namespace hash;

    HashBasedJoin::HashBasedJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *builder)
            : leftKeyIndex_(leftKeyIndex), rightKeyIndex_(rightKeyIndex), builder_(unique_ptr<JoinBuilder>(builder)),
              container_() {}


    shared_ptr<Table> HashBasedJoin::join(Table &left, Table &right) {
        function<unique_ptr<MemDataRow>(DataRow &)> snapshoter =
                bind(&JoinBuilder::snapshot, builder_.get(), std::placeholders::_1);
        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, snapshoter);
        auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());
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

        if (predicate_) {
            if (outer_) {
                for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = container_->get(leftval);
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    DataRow &right = result ? *result : MemDataRow::EMPTY;
                    if (predicate_(leftrow, right)) {
                        rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                    }
                }
            } else {
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
            }
        } else {
            if (outer_) {
                for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = container_->get(leftval);
                    DataRow &row = result ? (*leftrows)[leftkeys->pos()] : MemDataRow::EMPTY;
                    rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                }
            } else {
                for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = container_->get(leftval);
                    if (result) {
                        DataRow &row = (*leftrows)[leftkeys->pos()];
                        rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                    }
                }
            }
        }
        resultblock->resize(counter);
    }

    HashFilterJoin::HashFilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex) : leftKeyIndex_(leftKeyIndex),
                                                                                    rightKeyIndex_(rightKeyIndex) {}


    shared_ptr<Table> HashFilterJoin::join(Table &left, Table &right) {
        predicate_ = HashBuilder::buildHashPredicate(right, rightKeyIndex_);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashFilterJoin::probe, this, _1);
        return make_shared<TableView>(left.colSize(), left.blocks()->map(prober));
    }


    shared_ptr<Block> HashFilterJoin::probe(const shared_ptr<Block> &leftBlock) {
        auto col = leftBlock->col(leftKeyIndex_);
        auto bitmap = make_shared<SimpleBitmap>(leftBlock->limit());
        if (anti_) {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                auto key = col->next().asInt();
                if (!predicate_->test(key)) {
                    bitmap->put(col->pos());
                }
            }
        } else {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                auto key = col->next().asInt();
                if (predicate_->test(key)) {
                    bitmap->put(col->pos());
                }
            }
        }
        return leftBlock->mask(bitmap);
    }

    HashExistJoin::HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                                 JoinBuilder *builder, function<bool(DataRow &, DataRow &)> pred)
            : HashBasedJoin(leftKeyIndex, rightKeyIndex, builder), predicate_(pred) {}

    void HashExistJoin::probe(MemTable *output, const shared_ptr<Block> &leftBlock) {
        auto resultblock = output->allocate(container_->size());

        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();
        uint32_t counter = 0;
        auto writer = resultblock->rows();

        auto& content = container_->content();

        if (predicate_) {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataField &keyfield = leftkeys->next();
                auto key = keyfield.asInt();
                auto result = content.find(key);
                if (result != content.end()) {
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    if (predicate_(leftrow, *result->second)) {
                        (*writer)[counter++] = (*result->second);
                        content.erase(result);
                    }
                }
            }
        } else {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataField &keyfield = leftkeys->next();
                auto key = keyfield.asInt();
                auto result = container_->remove(key);
                if (result) {
                    (*writer)[counter++] = (*result);
                }
            }
        }
        resultblock->resize(counter);
    }

    HashNotExistJoin::HashNotExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, lqf::JoinBuilder *rowBuilder,
                                       function<bool(DataRow &, DataRow &)> pred) :
            HashBasedJoin(leftKeyIndex, rightKeyIndex, rowBuilder), predicate_(pred) {}

    shared_ptr<Table> HashNotExistJoin::join(Table &left, Table &right) {
        function<unique_ptr<MemDataRow>(DataRow &)> snapshoter =
                bind(&JoinBuilder::snapshot, builder_.get(), std::placeholders::_1);
        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, snapshoter);

        auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());

        function<void(const shared_ptr<Block> &)> prober = bind(&HashNotExistJoin::probe, this, memTable.get(), _1);
        left.blocks()->foreach(prober);

        auto resultBlock = memTable->allocate(container_->size());
        auto writeRows = resultBlock->rows();
        for (auto &entry: container_->content()) {
            writeRows->next() = *entry.second;
        }

        return memTable;
    }

    void HashNotExistJoin::probe(lqf::MemTable *output, const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        auto& content = container_->content();

        if (predicate_) {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataField &keyfield = leftkeys->next();
                auto key = keyfield.asInt();
                auto result = content.find(key);
                if (result != content.end()) {
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    if (predicate_(leftrow, *result->second)) {
                        content.erase(result);
                    }
                }
            }
        } else {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataField &keyfield = leftkeys->next();
                auto key = keyfield.asInt();
                auto result = container_->remove(key);
            }
        }
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

        if (outer_) {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataField &key = leftkeys->next();
                auto leftval = key.asInt();

                auto result = container_->get(leftval);
                (*writer)[i] = result ? *result : MemDataRow::EMPTY;
            }
        } else {
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataField &key = leftkeys->next();
                auto leftval = key.asInt();

                auto result = container_->get(leftval);
                (*writer)[i] = *result;
            }
        }

        auto newblock = owner->allocate(0);
        // Merge result block with original block
        auto newvblock = static_pointer_cast<MemvBlock>(newblock);

        newvblock->merge(*leftvBlock, columnBuilder_->leftInst());
        newvblock->merge(vblock, columnBuilder_->rightInst());
    }

    namespace powerjoin {

        PowerHashBasedJoin::PowerHashBasedJoin(function<int64_t(DataRow &)> left_key_maker,
                                               function<int64_t(DataRow &)> right_key_maker,
                                               lqf::JoinBuilder *builder,
                                               function<bool(DataRow &, DataRow &)> predicate) :
                left_key_maker_(left_key_maker), right_key_maker_(right_key_maker),
                builder_(unique_ptr<JoinBuilder>(builder)), predicate_(predicate) {}

        shared_ptr<Table> PowerHashBasedJoin::join(Table &left, Table &right) {
            function<unique_ptr<MemDataRow>(DataRow &)> snapshoter =
                    bind(&JoinBuilder::snapshot, builder_.get(), std::placeholders::_1);
            container_ = HashBuilder::buildContainer(right, right_key_maker_, snapshoter);
            auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());
            function<void(const shared_ptr<Block> &)> prober = bind(&PowerHashBasedJoin::probe, this, memTable.get(),
                                                                    _1);
            left.blocks()->foreach(prober);
            return memTable;
        }

        PowerHashJoin::PowerHashJoin(function<int64_t(DataRow &)> lkm, function<int64_t(DataRow &)> rkm,
                                     lqf::RowBuilder *rowBuilder, function<bool(DataRow &, DataRow &)> pred)
                : PowerHashBasedJoin(lkm, rkm, rowBuilder, pred),
                  rowBuilder_(rowBuilder) {}

        void PowerHashJoin::probe(MemTable *output, const shared_ptr<Block> &leftBlock) {
            auto resultblock = output->allocate(leftBlock->size());

            auto leftrows = leftBlock->rows();
            uint32_t counter = 0;
            auto writer = resultblock->rows();

            if (predicate_) {
                if (outer_) {
                    for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        DataRow &right = result ? *result : MemDataRow::EMPTY;
                        if (predicate_(leftrow, right)) {
                            rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                        }
                    }
                } else {
                    for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        if (result && predicate_(leftrow, *result)) {
                            rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                        }
                    }
                }
            } else {
                if (outer_) {
                    for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                    }
                } else {
                    for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        if (result) {
                            rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                        }
                    }
                }
            }
            resultblock->resize(counter);
        }

        PowerHashColumnJoin::PowerHashColumnJoin(function<int64_t(DataRow &)> lkm, function<int64_t(DataRow &)> rkm,
                                                 lqf::ColumnBuilder *colBuilder)
                : PowerHashBasedJoin(lkm, rkm, colBuilder), columnBuilder_(colBuilder) {}


        void PowerHashColumnJoin::probe(lqf::MemTable *owner, const shared_ptr<Block> &leftBlock) {
            shared_ptr<MemvBlock> leftvBlock = static_pointer_cast<MemvBlock>(leftBlock);
            /// Make sure the cast is valid
            assert(leftvBlock.get() != nullptr);

            auto leftrows = leftBlock->rows();

            MemvBlock vblock(leftBlock->size(), columnBuilder_->rightColSize());
            auto writer = vblock.rows();
            for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                DataRow &leftrow = leftrows->next();
                auto leftkey = left_key_maker_(leftrow);
                auto result = container_->get(leftkey);
                (*writer)[i] = *result;
            }

            auto newblock = owner->allocate(0);
            // Merge result block with original block
            auto newvblock = static_pointer_cast<MemvBlock>(newblock);

            newvblock->merge(*leftvBlock, columnBuilder_->leftInst());
            newvblock->merge(vblock, columnBuilder_->rightInst());
        }

        PowerHashFilterJoin::PowerHashFilterJoin(function<int64_t(DataRow &)> left_key_maker,
                                                 function<int64_t(DataRow &)> right_key_maker)
                : left_key_maker_(left_key_maker), right_key_maker_(right_key_maker) {}

        shared_ptr<Block> PowerHashFilterJoin::probe(const shared_ptr<Block> &leftBlock) {
            auto bitmap = make_shared<SimpleBitmap>(leftBlock->limit());
            auto rows = leftBlock->rows();
            if (anti_) {
                for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                    auto key = left_key_maker_(rows->next());
                    if (!predicate_->test(key)) {
                        bitmap->put(rows->pos());
                    }
                }
            } else {
                for (uint32_t i = 0; i < leftBlock->size(); ++i) {
                    auto key = left_key_maker_(rows->next());
                    if (predicate_->test(key)) {
                        bitmap->put(rows->pos());
                    }
                }
            }
            return leftBlock->mask(bitmap);
        }

        shared_ptr<Table> PowerHashFilterJoin::join(Table &left, Table &right) {
            predicate_ = HashBuilder::buildHashPredicate(right, right_key_maker_);

            function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&PowerHashFilterJoin::probe, this, _1);
            return make_shared<TableView>(left.colSize(), left.blocks()->map(prober));
        }

    }
}
