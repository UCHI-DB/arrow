//
// Created by harper on 2/25/20.
//

#include "join.h"
#include "rowcopy.h"

using namespace std;
using namespace std::placeholders;
using namespace lqf::rowcopy;

namespace lqf {
    namespace join {

        JoinBuilder::JoinBuilder(initializer_list<int32_t> initList, bool needkey, bool vertical) :
                needkey_(needkey), vertical_(vertical), field_list_(initList), left_type_(I_OTHER),
                right_type_(I_OTHER) {}

        void JoinBuilder::on(Table &left, Table &right) {
            left_type_ = table_type(left);
            right_type_ = table_type(right);

            auto last = 0U;
            left_col_offset_.push_back(last);
            for (auto &s:left.colSize()) {
                auto insert = last + s;
                left_col_offset_.push_back(insert);
                last = insert;
            }
            last = 0U;
            right_col_offset_.push_back(last);
            for (auto &s:right.colSize()) {
                auto insert = last + s;
                right_col_offset_.push_back(insert);
                last = insert;
            }
        }

        void JoinBuilder::init() {}

        unique_ptr<MemDataRow> JoinBuilder::snapshot(DataRow &input) {
            if (!init_) {
                throw "not init";
            }
            auto res = unique_ptr<MemDataRow>(new MemDataRow(snapshot_col_offset_));
            (*snapshot_copier_)(*res, input);
            return res;
        }

        RowBuilder::RowBuilder(initializer_list<int32_t> fields, bool needkey, bool vertical)
                : JoinBuilder(fields, needkey, vertical) {}

        void RowBuilder::init() {
            init_ = true;
            uint32_t i = needkey_;
            uint32_t right_counter = 0;

            RowCopyFactory snapshot_factory;
            RowCopyFactory left_factory;
            RowCopyFactory right_factory;

            snapshot_factory.from(right_type_);
            snapshot_factory.to(I_RAW);

            auto dest_type = vertical_ ? I_OTHER : I_RAW;
            left_factory.from(left_type_);
            left_factory.to(dest_type);
            right_factory.from(I_RAW);
            right_factory.to(dest_type);

            output_col_offset_.push_back(0);
            if (needkey_) {
                output_col_offset_.push_back(1);
                output_col_size_.push_back(1);
            }
            snapshot_col_offset_.push_back(0);

            for (auto &inst: field_list_) {
                auto index = inst & 0xffff;
                bool is_string = inst >> 17;
                bool is_raw = inst >> 18;
                bool is_right = inst & 0x10000;

                output_col_size_.push_back(is_string ? 2 : 1);
                output_col_offset_.push_back(output_col_offset_.back() + output_col_size_.back());

                if (is_right) {
                    FIELD_TYPE ifield_type;
                    FIELD_TYPE ofield_type;
                    if (is_raw) {
                        ifield_type = F_RAW;
                        ofield_type = F_REGULAR;
                    } else if (is_string) {
                        ifield_type = F_STRING;
                        ofield_type = F_STRING;
                    } else {
                        ifield_type = F_REGULAR;
                        ofield_type = F_REGULAR;
                    }
                    snapshot_factory.field(ifield_type, index, right_counter);
                    right_factory.field(ofield_type, right_counter++, i);

                    snapshot_col_size_.push_back(output_col_size_.back());
                    snapshot_col_offset_.push_back(snapshot_col_offset_.back() + snapshot_col_size_.back());
                } else {
                    FIELD_TYPE field_type;
                    if (is_raw) {
                        field_type = F_RAW;
                    } else if (is_string) {
                        field_type = F_STRING;
                    } else {
                        field_type = F_REGULAR;
                    }
                    left_factory.field(field_type, index, i);
                }
                ++i;
            }
            snapshot_factory.from_layout(right_col_offset_);
            snapshot_factory.to_layout(snapshot_col_offset_);

            left_factory.from_layout(left_col_offset_);
            left_factory.to_layout(output_col_offset_);

            right_factory.from_layout(snapshot_col_offset_);
            right_factory.to_layout(output_col_offset_);

            snapshot_copier_ = snapshot_factory.build();
            left_copier_ = left_factory.build();
            right_copier_ = right_factory.build();
        }

        void RowBuilder::build(DataRow &output, DataRow &left, DataRow &right, int key) {
            if (!init_)
                throw "not init";

            if (needkey_) {
                output[0] = key;
            }
            (*left_copier_)(output, left);
            (*right_copier_)(output, right);
        }

        ColumnBuilder::ColumnBuilder(initializer_list<int32_t> fields)
                : JoinBuilder(fields, false, true) {}

        void ColumnBuilder::init() {
            init_ = true;

            RowCopyFactory snapshot_factory;

            snapshot_factory.from(right_type_);
            snapshot_factory.to(I_RAW);

            uint32_t i = 0;
            uint32_t right_counter = 0;

            output_col_offset_.push_back(0);
            snapshot_col_offset_.push_back(0);

            for (auto &inst: field_list_) {
                auto index = inst & 0xffff;
                bool is_string = inst >> 17;
                bool is_raw = inst >> 18;
                bool is_right = inst & 0x10000;

                output_col_size_.push_back(is_string ? 2 : 1);
                output_col_offset_.push_back(output_col_offset_.back() + output_col_size_.back());

                if (is_right) {
                    FIELD_TYPE ifield_type;
                    if (is_raw) {
                        ifield_type = F_RAW;
                    } else if (is_string) {
                        ifield_type = F_STRING;
                    } else {
                        ifield_type = F_REGULAR;
                    }
                    snapshot_factory.field(ifield_type, index, right_counter);
                    right_merge_inst_.emplace_back(right_counter++, i);

                    snapshot_col_size_.push_back(output_col_size_.back());
                    snapshot_col_offset_.push_back(snapshot_col_offset_.back() + snapshot_col_size_.back());
                } else {
                    left_merge_inst_.emplace_back(index, i);
                }
                ++i;
            }
            snapshot_factory.from_layout(right_col_offset_);
            snapshot_factory.to_layout(snapshot_col_offset_);

            snapshot_copier_ = snapshot_factory.build();
        }

        void ColumnBuilder::build(MemvBlock &output, MemvBlock &left, MemvBlock &right) {
            if (!init_) {
                throw "not init";
            }
            output.merge(left, left_merge_inst_);
            output.merge(right, right_merge_inst_);
        }
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
            : leftKeyIndex_(leftKeyIndex), rightKeyIndex_(rightKeyIndex),
              builder_(unique_ptr<JoinBuilder>(builder)),
              container_() {}


    shared_ptr<Table> HashBasedJoin::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();

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
        return make_shared<TableView>(&left, left.colSize(), left.blocks()->map(prober));
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
        builder_->on(left, right);
        builder_->init();

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
            function<void(const shared_ptr<Block> &)> prober = bind(&HashExistJoin::probe, this, memTable.get(),
                                                                    _1);
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
        builder_->on(left, right);
        builder_->init();

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
            function<void(const shared_ptr<Block> &)> prober = bind(&HashNotExistJoin::probe, this, memTable.get(),
                                                                    _1);
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
                if (result)
                    (*writer)[i] = *result;
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

        columnBuilder_->build(*newvblock, *leftvBlock, vblock);
//        newvblock->merge(*leftvBlock, columnBuilder_->leftInst());
//        newvblock->merge(vblock, columnBuilder_->rightInst());
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
            function<void(const shared_ptr<Block> &)> prober = bind(&PowerHashBasedJoin::probe, this,
                                                                    memTable.get(),
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

            columnBuilder_->build(*newvblock, *leftvBlock, vblock);
//            newvblock->merge(*leftvBlock, columnBuilder_->leftInst());
//            newvblock->merge(vblock, columnBuilder_->rightInst());
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

            function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&PowerHashFilterJoin::probe, this,
                                                                                 _1);
            return make_shared<TableView>(&left, left.colSize(), left.blocks()->map(prober));
        }

    }
}
