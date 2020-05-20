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


    using namespace join;
    using namespace hashcontainer;

    Join::Join() : Node(2) {}

    unique_ptr<NodeOutput> Join::execute(const vector<NodeOutput *> &inputs) {
        auto left = static_cast<TableOutput *>(inputs[0]);
        auto right = static_cast<TableOutput *>(inputs[1]);

        auto result = join(*(left->get()), *(right->get()));
        return unique_ptr<TableOutput>(new TableOutput(result));
    }

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

    FilterJoin::FilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, uint32_t expect_size, bool useBitmap)
            : leftKeyIndex_(leftKeyIndex), rightKeyIndex_(rightKeyIndex), expect_size_(expect_size),
              useBitmap_(useBitmap) {}


    shared_ptr<Table> FilterJoin::join(Table &left, Table &right) {
#ifdef LQF_NODE_TIMING
        auto start = high_resolution_clock::now();
#endif
        if (useBitmap_) {
            predicate_ = HashBuilder::buildBitmapPredicate(right, rightKeyIndex_, expect_size_);
        } else {
            predicate_ = HashBuilder::buildHashPredicate(right, rightKeyIndex_, expect_size_);
        }

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&FilterJoin::probe, this, _1);
#ifdef LQF_NODE_TIMING
        auto stop = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(stop - start);
        cout << "Filter Join " << name_ << " Time taken: " << duration.count() << " microseconds" << endl;
#endif
        return make_shared<TableView>(left.colSize(), left.blocks()->map(prober));
    }


    shared_ptr<Block> FilterJoin::probe(const shared_ptr<Block> &leftBlock) {
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
