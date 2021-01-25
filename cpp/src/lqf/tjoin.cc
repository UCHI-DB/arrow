//
// Created by harper on 1/24/21.
//

#include "tjoin.h"

using namespace std;
using namespace std::placeholders;
using namespace lqf::rowcopy;

namespace lqf {

    /**
     * Instantiate templates
     */
    template
    class FilterTJoin<Hash32Predicate>;

    template
    class FilterTJoin<Hash32SetPredicate>;

    template
    class HashBasedTJoin<Hash32SparseContainer>;

    template
    class HashBasedTJoin<Hash32DenseContainer>;

    template
    class HashBasedTJoin<Hash32MapHeapContainer>;

    template
    class HashBasedTJoin<Hash32MapPageContainer>;

    template
    class HashTJoin<Hash32SparseContainer>;

    template
    class HashTJoin<Hash32DenseContainer>;

    template
    class HashTJoin<Hash32MapHeapContainer>;

    template
    class HashTJoin<Hash32MapPageContainer>;

    template
    class HashColumnTJoin<Hash32SparseContainer>;

    template
    class HashColumnTJoin<Hash32DenseContainer>;

    template
    class HashColumnTJoin<Hash32MapHeapContainer>;

    template
    class HashColumnTJoin<Hash32MapPageContainer>;

    template
    class ParquetHashColumnTJoin<Hash32SparseContainer>;

    template
    class ParquetHashColumnTJoin<Hash32DenseContainer>;

    template
    class ParquetHashColumnTJoin<Hash32MapHeapContainer>;

    template
    class ParquetHashColumnTJoin<Hash32MapPageContainer>;

    template<typename Container>
    HashBasedTJoin<Container>::HashBasedTJoin(uint32_t lki, uint32_t rki, JoinBuilder *builder,
                                              uint32_t expect_size)
            : left_key_index_(lki), right_key_index_(rki),
              builder_(unique_ptr<JoinBuilder>(builder)), expect_size_(expect_size) {}

    template<typename Container>
    shared_ptr<Table> HashBasedTJoin<Container>::join(Table &left, Table &right) {
        builder_->on(left, right);
        builder_->init();
        container_ = ContainerBuilder::build<Container>(right, right_key_index_, builder_->snapshoter(),
                                                        expect_size_);
        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&HashBasedTJoin<Container>::probe,
                                                                             this, _1);
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

    template<typename Container>
    HashTJoin<Container>::HashTJoin(uint32_t lk, uint32_t rk, lqf::RowBuilder *builder,
                                    function<bool(DataRow &, DataRow &)> pred, uint32_t expect_size)
            : HashBasedTJoin<Container>(lk, rk, builder, expect_size), rowBuilder_(builder), predicate_(pred) {}

    template<typename Container>
    shared_ptr<Block> HashTJoin<Container>::probe(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(HashBasedTJoin<Container>::left_key_index_);
        auto leftrows = leftBlock->rows();
        auto resultblock = HashBasedTJoin<Container>::makeBlock(leftBlock->size());
        uint32_t counter = 0;
        auto writer = resultblock->rows();

        auto left_block_size = leftBlock->size();
        if (predicate_) {
            if (HashBasedTJoin<Container>::outer_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = HashBasedTJoin<Container>::container_->get(leftval);
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
                    auto result = HashBasedTJoin<Container>::container_->get(leftval);
                    if (result) {
                        DataRow &row = (*leftrows)[leftkeys->pos()];
                        if (predicate_(row, *result)) {
                            rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                        }
                    }
                }
            }
        } else {
            if (HashBasedTJoin<Container>::outer_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    DataRow &leftrow = (*leftrows)[leftkeys->pos()];
                    auto result = HashBasedTJoin<Container>::container_->get(leftval);
                    DataRow &right = result ? *result : MemDataRow::EMPTY;
                    rowBuilder_->build((*writer)[counter++], leftrow, right, leftval);
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = HashBasedTJoin<Container>::container_->get(leftval);
                    if (result) {
                        DataRow &row = (*leftrows)[leftkeys->pos()];
                        rowBuilder_->build((*writer)[counter++], row, *result, leftval);
                    }
                }
            }
        }
        resultblock->resize(counter);

        return resultblock;
    }

    template<typename Container>
    HashColumnTJoin<Container>::HashColumnTJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                                                lqf::ColumnBuilder *builder,
                                                bool need_filter, uint32_t expect_size)
            : HashBasedTJoin<Container>(leftKeyIndex, rightKeyIndex, builder, expect_size), need_filter_(need_filter),
              columnBuilder_(builder) {}

    template<typename Container>
    shared_ptr<Block> HashColumnTJoin<Container>::probe(const shared_ptr<Block> &leftBlock) {
        shared_ptr<MemvBlock> leftvBlock = dynamic_pointer_cast<MemvBlock>(leftBlock);
        /// Make sure the cast is valid
        assert(leftvBlock.get() != nullptr);

        auto leftkeys = leftBlock->col(HashBasedTJoin<Container>::left_key_index_);

        MemvBlock vblock(leftBlock->size(), columnBuilder_->rightColSize());
        auto writer = vblock.rows();

        auto left_block_size = leftBlock->size();

        shared_ptr<Bitmap> filter;
        if (need_filter_) {
            filter = make_shared<SimpleBitmap>(left_block_size);
        }

        if (HashBasedTJoin<Container>::outer_) {
            for (uint32_t i = 0; i < left_block_size; ++i) {
                DataField &key = leftkeys->next();
                auto leftval = key.asInt();

                auto result = HashBasedTJoin<Container>::container_->get(leftval);
                if (result)
                    (*writer)[i] = *result;
            }
        } else {
            if (need_filter_) {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();

                    auto result = HashBasedTJoin<Container>::container_->get(leftval);
                    if (result) {
                        (*writer)[i] = *move(result);
                    } else {
                        // mask filter
                        filter->put(i);
                    }
                }
            } else {
                for (uint32_t i = 0; i < left_block_size; ++i) {
                    DataField &key = leftkeys->next();
                    auto leftval = key.asInt();
                    auto result = move(HashBasedTJoin<Container>::container_->get(leftval));
                    (*writer)[i] = *result;
                }
            }
        }

        auto newblock = HashBasedTJoin<Container>::makeBlock(0);
        // Merge result block with original block
        auto newvblock = static_pointer_cast<MemvBlock>(newblock);

        columnBuilder_->build(*newvblock, *leftvBlock, vblock);
        if (need_filter_) {
            return newvblock->mask(~(*filter));
        }
        return newvblock;
    }

    template<typename Container>
    ParquetHashColumnTJoin<Container>::ParquetHashColumnTJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                                                              lqf::ColumnBuilder *builder, uint32_t expect_size)
            : HashColumnTJoin<Container>(leftKeyIndex, rightKeyIndex, builder, true, expect_size) {}

    template<typename Container>
    shared_ptr<Block> ParquetHashColumnTJoin<Container>::probe(const shared_ptr<Block> &leftBlock) {
        auto leftkeys = leftBlock->col(HashBasedTJoin<Container>::left_key_index_);

        MemvBlock probed(leftBlock->size(), HashColumnTJoin<Container>::columnBuilder_->rightColSize());
        auto writer = probed.rows();

        auto left_block_size = leftBlock->size();

        auto filter = make_shared<SimpleBitmap>(left_block_size);

        uint32_t counter = 0;
        for (uint32_t i = 0; i < left_block_size; ++i) {
            DataField &key = leftkeys->next();
            auto leftval = key.asInt();

            auto result = HashBasedTJoin<Container>::container_->get(leftval);
            if (result) {
                (*writer)[counter++] = *move(result);
            } else {
                // mask filter
                filter->put(i);
            }
        }
        probed.resize(counter);
        ~(*filter);
        auto maskedParquet = leftBlock->mask(filter);
        // Load columns into memory from left side
        auto leftCacheBlock = HashColumnTJoin<Container>::columnBuilder_->cacheToMem(*maskedParquet);

        auto newblock = HashBasedTJoin<Container>::makeBlock(0);
        // Merge result block with original block
        auto newvblock = static_pointer_cast<MemvBlock>(newblock);

        HashColumnTJoin<Container>::columnBuilder_->buildFromMem(*newvblock, *leftCacheBlock, probed);
        return newvblock;
    }

    template<typename Container>
    FilterTJoin<Container>::FilterTJoin(uint32_t lki, uint32_t rki, uint32_t expect_size)
            : left_key_index_(lki), right_key_index_(rki), expect_size_(expect_size) {}


    template<typename Container>
    shared_ptr<Table> FilterTJoin<Container>::join(Table &left, Table &right) {
#ifdef LQF_NODE_TIMING
        auto start = high_resolution_clock::now();
#endif
        predicate_ = PredicateBuilder::build<Container>(right, right_key_index_, expect_size_);

        function<shared_ptr<Block>(const shared_ptr<Block> &)> prober = bind(&FilterTJoin<Container>::probe, this, _1);
#ifdef LQF_NODE_TIMING
        auto stop = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(stop - start);
        cout << "Filter Join " << name_ << " Time taken: " << duration.count() << " microseconds" << endl;
#endif
        return make_shared<TableView>(left.type(), left.colSize(), left.blocks()->map(prober));
    }

    template <typename Container>
    shared_ptr<Block> FilterTJoin<Container>::probe(const shared_ptr<Block> &leftBlock) {
        auto col = leftBlock->col(left_key_index_);
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
}