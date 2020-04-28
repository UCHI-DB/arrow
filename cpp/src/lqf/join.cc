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
            uint32_t right_counter = 0;

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
                        right_read_raw_.push_back(pair<uint8_t, uint8_t>(index, right_counter));
                    } else {
                        right_read_inst_.push_back(pair<uint8_t, uint8_t>(index, right_counter));
                    }
                    right_write_inst_.push_back(pair<uint8_t, uint8_t>(right_counter++, i));
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
            for (auto &inst: right_read_inst_) {
                (*res)[inst.second] = input[inst.first];
            }
            for (auto &inst:right_read_raw_) {
                (*res)[inst.second] = input(inst.first);
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

            uint32_t rinst_size = right_write_inst_.size();
            for (uint32_t i = 0; i < rinst_size; ++i) {
                output[right_write_inst_[i].second] = right[i];
            }
        }

        ColumnBuilder::ColumnBuilder(initializer_list<int32_t> fields)
                : JoinBuilder(fields, false, true) {}

    }
    namespace hashjoin {

        template<typename DTYPE>
        HashPredicate<DTYPE>::HashPredicate():min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE>
        void HashPredicate<DTYPE>::add(ktype val) {
            // Update range
            ktype current;
            do {
                current = min_.load();
            } while (!min_.compare_exchange_strong(current, std::min(val, current)));
            do {
                current = max_.load();
            } while (!max_.compare_exchange_strong(current, std::max(val, current)));
            content_.add(val);
        }

        template<typename DTYPE>
        bool HashPredicate<DTYPE>::test(ktype val) {
            if (val >= min_ && val <= max_) {
                return content_.test(val);
            }
            return false;
        }

        template
        class HashPredicate<Int32>;

        template
        class HashPredicate<Int64>;

        BitmapPredicate::BitmapPredicate(uint32_t size) : bitmap_(size) {}

        void BitmapPredicate::add(int32_t val) {
            bitmap_.put(val);
        }

        bool BitmapPredicate::test(int32_t val) {
            return bitmap_.check(val);
        }

        template<typename DTYPE>
        HashContainer<DTYPE>::HashContainer() : hashmap_(), min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE>
        void HashContainer<DTYPE>::add(ktype key, unique_ptr<MemDataRow> dataRow) {
            ktype current;
            do {
                current = min_.load();
            } while (!min_.compare_exchange_strong(current, std::min(key, current)));
            do {
                current = max_.load();
            } while (!max_.compare_exchange_strong(current, std::max(key, current)));
            hashmap_.put(key, dataRow.release());
        }

        template<typename DTYPE>
        MemDataRow *HashContainer<DTYPE>::get(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return hashmap_.get(key);
        }

        template<typename DTYPE>
        unique_ptr<MemDataRow> HashContainer<DTYPE>::remove(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            auto result = hashmap_.remove(key);
            if (result != nullptr) {
                return unique_ptr<MemDataRow>(result);
            }
            return nullptr;
        }

        template<typename DTYPE>
        bool HashContainer<DTYPE>::test(ktype key) {
            return hashmap_.get(key) != nullptr;
        }

        template<typename DTYPE>
        unique_ptr<Iterator<pair<typename DTYPE::type, MemDataRow *>>> HashContainer<DTYPE>::iterator() {
            return hashmap_.iterator();
        }

        template
        class HashContainer<Int32>;

        template
        class HashContainer<Int64>;

        template<typename CONTENT>
        HashMemBlock<CONTENT>::HashMemBlock(shared_ptr<CONTENT> predicate) : MemBlock(0, 0) {
            content_ = move(predicate);
        }

        template<typename CONTENT>
        shared_ptr<CONTENT> HashMemBlock<CONTENT>::content() {
            return content_;
        }

        template
        class HashMemBlock<Int32Predicate>;

        template
        class HashMemBlock<Int64Predicate>;

        template
        class HashMemBlock<Hash32Predicate>;

        template
        class HashMemBlock<Hash64Predicate>;

        template
        class HashMemBlock<Hash32Container>;

        template
        class HashMemBlock<Hash64Container>;

        shared_ptr<Int32Predicate> HashBuilder::buildHashPredicate(Table &input, uint32_t keyIndex) {
            Hash32Predicate *pred = new Hash32Predicate();
            shared_ptr<Int32Predicate> retval = shared_ptr<Int32Predicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, keyIndex](const shared_ptr<Block> &block) {
                        auto hashblock = dynamic_pointer_cast<HashMemBlock<Int32Predicate>>(block);
                        if (hashblock) {
                            retval = hashblock->content();
                            return;
                        }
                        auto col = block->col(keyIndex);
                        uint32_t block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            pred->add(col->next().asInt());
                        }
                    };
            input.blocks()->foreach(processor);
            return retval;
        }

        shared_ptr<Int64Predicate>
        HashBuilder::buildHashPredicate(Table &input, function<int64_t(DataRow &)> key_maker) {
            Hash64Predicate *pred = new Hash64Predicate();
            shared_ptr<Int64Predicate> retval = shared_ptr<Int64Predicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, key_maker](const shared_ptr<Block> &block) {
                        auto hashblock = dynamic_pointer_cast<HashMemBlock<Int64Predicate>>(block);
                        if (hashblock) {
                            retval = hashblock->content();
                            return;
                        }
                        auto rows = block->rows();
                        auto block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            DataRow &row = rows->next();
                            pred->add(key_maker(row));
                        }
                    };
            input.blocks()->foreach(processor);
            return retval;
        }

        shared_ptr<Int32Predicate> HashBuilder::buildBitmapPredicate(Table &input, uint32_t keyIndex) {
            ParquetTable &ptable = (ParquetTable &) input;

            auto predicate = new BitmapPredicate(ptable.size());
            function<void(const shared_ptr<Block> &)> processor = [=](const shared_ptr<Block> &block) {
                auto col = block->col(keyIndex);
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    auto key = col->next().asInt();
                    predicate->add(key);
                }
            };
            input.blocks()->foreach(processor);
            return shared_ptr<Int32Predicate>(predicate);
        }

        shared_ptr<Hash32Container> HashBuilder::buildContainer(Table &input, uint32_t keyIndex,
                                                                function<unique_ptr<MemDataRow>(DataRow &)> builder) {
            Hash32Container *container = new Hash32Container();
            shared_ptr<Hash32Container> retval = shared_ptr<Hash32Container>(container);

            function<void(const shared_ptr<Block> &)> processor = [builder, keyIndex, &container, &retval](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock<Hash32Container>>(block);
                if (hashblock) {
                    retval = hashblock->content();
                    return;
                }
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    auto key = row[keyIndex].asInt();
                    container->add(key, builder(row));
                }
            };
            input.blocks()->foreach(processor);
            return retval;
        }

        shared_ptr<Hash64Container> HashBuilder::buildContainer(Table &input,
                                                                function<int64_t(DataRow &)> key_maker,
                                                                function<unique_ptr<MemDataRow>(DataRow &)> builder) {
            Hash64Container *container = new Hash64Container();
            shared_ptr<Hash64Container> retval = shared_ptr<Hash64Container>(container);
            function<void(const shared_ptr<Block> &)> processor = [builder, &container, &retval, key_maker](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock<Hash64Container>>(block);
                if (hashblock) {
                    retval = hashblock->content();
                }
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    auto key = key_maker(row);
                    container->add(key, builder(row));
                }
            };
            input.blocks()->foreach(processor);
            return retval;
        }
    }

    using namespace join;
    using namespace hashjoin;

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

        auto left_block_size = leftBlock->size();
        if (predicate_) {
            if (outer_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
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
                for (uint32_t i = 0; i < left_block_size; ++i) {
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
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    auto result = container_->get(leftval);
                    DataRow &right = result ? *result : MemDataRow::EMPTY;
                    rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
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
        uint32_t size = leftBlock->size();
        if (anti_) {
            for (uint32_t i = 0; i < size; ++i) {
                auto key = col->next().asInt();
                if (!predicate_->test(key)) {
                    bitmap->put(col->pos());
                }
            }
        } else {
            for (uint32_t i = 0; i < size; ++i) {
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

    shared_ptr<Table> HashExistJoin::join(Table &left, Table &right) {
        function<unique_ptr<MemDataRow>(DataRow &)> snapshoter =
                bind(&JoinBuilder::snapshot, builder_.get(), std::placeholders::_1);
        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, snapshoter);

        auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());

        if (predicate_) {
            function<shared_ptr<Bitmap>(const shared_ptr<Block> &)> prober = bind(
                    &HashExistJoin::probeWithPredicate, this, _1);
            function<shared_ptr<Bitmap>(shared_ptr<Bitmap> &, shared_ptr<Bitmap> &)> reducer =
                    [](shared_ptr<Bitmap> &a, shared_ptr<Bitmap> &b) {
                        return (*a) | (*b);
                    };
            auto exist = left.blocks()->map(prober)->reduce(reducer);
            auto memblock = memTable->allocate(exist->cardinality());

            auto writerows = memblock->rows();
            auto writecount = 0;


            auto bitmapite = exist->iterator();
            while (bitmapite->hasNext()) {
                (*writerows)[writecount++] = *container_->get(static_cast<int32_t>(bitmapite->next()));
            }
        } else {
            function<void(const shared_ptr<Block> &)> prober = bind(&HashExistJoin::probe, this, memTable.get(), _1);
            left.blocks()->foreach(prober);
        }

        return memTable;
    }

    shared_ptr<SimpleBitmap> HashExistJoin::probeWithPredicate(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        auto left_block_size = leftBlock->size();

        shared_ptr<SimpleBitmap> local_mask = make_shared<SimpleBitmap>(container_->max() + 1);

        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &keyfield = leftkeys->next();
            auto key = keyfield.asInt();
            if (!local_mask->check(key)) {
                auto result = container_->get(key);
                if (result != nullptr) {
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    if (predicate_(leftrow, *result)) {
                        local_mask->put(key);
                    }
                }
            }
        }
        return local_mask;
    }

    void HashExistJoin::probe(MemTable *output, const shared_ptr<Block> &leftBlock) {
        auto resultblock = output->allocate(container_->size());
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        uint32_t counter = 0;
        auto writer = resultblock->rows();
        auto left_block_size = leftBlock->size();

        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &keyfield = leftkeys->next();
            auto result = container_->remove(keyfield.asInt());
            if (result) {
                (*writer)[counter++] = (*result);
            }
        }
        resultblock->resize(counter);
    }

    HashNotExistJoin::HashNotExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, lqf::JoinBuilder *rowBuilder,
                                       function<bool(DataRow &, DataRow &)> pred) :
            HashExistJoin(leftKeyIndex, rightKeyIndex, rowBuilder, pred) {}

    shared_ptr<Table> HashNotExistJoin::join(Table &left, Table &right) {
        function<unique_ptr<MemDataRow>(DataRow &)> snapshoter =
                bind(&JoinBuilder::snapshot, builder_.get(), std::placeholders::_1);
        container_ = HashBuilder::buildContainer(right, rightKeyIndex_, snapshoter);

        auto memTable = MemTable::Make(builder_->outputColSize(), builder_->useVertical());

        if (predicate_) {
            function<shared_ptr<Bitmap>(const shared_ptr<Block> &)> prober = bind(
                    &HashNotExistJoin::probeWithPredicate, this, _1);
            function<shared_ptr<Bitmap>(shared_ptr<Bitmap> &, shared_ptr<Bitmap> &)> reducer =
                    [](shared_ptr<Bitmap> &a, shared_ptr<Bitmap> &b) {
                        return (*a) | (*b);
                    };
            auto exist = left.blocks()->map(prober)->reduce(reducer);
            auto memblock = memTable->allocate(container_->size() - exist->cardinality());

            auto writerows = memblock->rows();
            auto writecount = 0;

            auto ite = container_->iterator();
            while (ite->hasNext()) {
                auto entry = ite->next();
                if (!exist->check(entry.first)) {
                    (*writerows)[writecount++] = *(entry.second);
                }
            }
        } else {
            function<void(const shared_ptr<Block> &)> prober = bind(&HashNotExistJoin::probe, this, memTable.get(), _1);
            left.blocks()->foreach(prober);

            auto resultBlock = memTable->allocate(container_->size());
            auto writeRows = resultBlock->rows();
            auto iterator = container_->iterator();
            while (iterator->hasNext()) {
                auto entry = iterator->next();
                writeRows->next() = *entry.second;
            }
        }
        return memTable;
    }

    void HashNotExistJoin::probe(lqf::MemTable *output, const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(leftKeyIndex_);
        auto leftrows = leftBlock->rows();

        auto left_block_size = leftBlock->size();
        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &keyfield = leftkeys->next();
            container_->remove(keyfield.asInt());
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

        auto left_block_size = leftBlock->size();
        if (outer_) {
            for (uint32_t i = 0; i < left_block_size; ++i) {
                DataField &key = leftkeys->next();
                auto leftval = key.asInt();

                auto result = container_->get(leftval);
                (*writer)[i] = result ? *result : MemDataRow::EMPTY;
            }
        } else {
            for (uint32_t i = 0; i < left_block_size; ++i) {
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
            auto left_block_size = leftBlock->size();
            if (predicate_) {
                if (outer_) {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        DataRow &right = result ? *result : MemDataRow::EMPTY;
                        if (predicate_(leftrow, right)) {
                            rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                        }
                    }
                } else {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
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
                    for (uint32_t i = 0; i < left_block_size; ++i) {
                        DataRow &leftrow = leftrows->next();
                        auto leftval = left_key_maker_(leftrow);
                        auto result = container_->get(leftval);
                        rowBuilder_->build((*writer)[counter++], leftrow, *result, leftval);
                    }
                } else {
                    for (uint32_t i = 0; i < left_block_size; ++i) {
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
            auto left_block_size = leftBlock->size();
            for (uint32_t i = 0; i < left_block_size; ++i) {
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
            auto left_block_size = leftBlock->size();
            if (anti_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    auto key = left_key_maker_(rows->next());
                    if (!predicate_->test(key)) {
                        bitmap->put(rows->pos());
                    }
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
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
