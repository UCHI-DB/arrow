//
// Created by harper on 2/3/21.
//

#include "operator_enc.h"

namespace lqf {
    namespace encopr {

        EncHashJoin::EncHashJoin(uint32_t l, uint32_t r, RowBuilder *rb,
                                 initializer_list<parquet::Type::type> data_type,
                                 initializer_list<encoding::EncodingType> enc_type,
                                 function<bool(DataRow &, DataRow &)> pred,
                                 uint32_t expect_size) : HashJoin(l, r, rb, pred, expect_size), data_types_(data_type),
                                                         enc_types_(enc_type) {}

        shared_ptr<Block> EncHashJoin::makeBlock(uint32_t size) {
            return make_shared<EncMemvBlock>(data_types_, enc_types_);
        }

        template<typename Container>
        EncHashTJoin<Container>::EncHashTJoin(uint32_t l, uint32_t r, RowBuilder *rb,
                                              initializer_list<parquet::Type::type> data_type,
                                              initializer_list<encoding::EncodingType> enc_type,
                                              function<bool(DataRow &, DataRow &)> pred,
                                              uint32_t expect_size) : HashTJoin<Container>(l, r, rb, pred, expect_size),
                                                                      data_types_(data_type),
                                                                      enc_types_(enc_type) {}

        template<typename Container>
        shared_ptr<Block> EncHashTJoin<Container>::makeBlock(uint32_t size) {
            return make_shared<EncMemvBlock>(data_types_, enc_types_);
        }

        template
        class EncHashTJoin<Hash32SparseContainer>;

        template
        class EncHashTJoin<Hash32DenseContainer>;

        template
        class EncHashTJoin<Hash32MapHeapContainer>;

        template
        class EncHashTJoin<Hash32MapPageContainer>;

        EncMemBetweenPredicate::EncMemBetweenPredicate(uint32_t index, int32_t from, int32_t to)
                : ColPredicate(index), from_(from), to_(to) {}

        shared_ptr<Bitmap> EncMemBetweenPredicate::filterBlock(Block &block, Bitmap &prev) {
            auto emvblock = dynamic_cast<EncMemvBlock *>(&block);
            auto block_size = emvblock->size();
            auto bitmap = make_shared<SimpleBitmap>(block_size);

            auto coldata = emvblock->rawcol(index_);

            auto dictbuffer = (*coldata)[coldata->size() - 1];

            Int32Dictionary dict(dictbuffer->mutable_data(), dictbuffer->size() >> 3);
            auto rawFrom = dict.lookup(from_);
            if (rawFrom < 0) {
                rawFrom = -rawFrom;
            }
            auto rawTo = dict.lookup(to_);
            if (rawTo < 0) {
                rawTo = -rawTo;
            }

            uint64_t counter = 0;
            for (int i = 0; i < coldata->size() - 1; ++i) {
                auto block = (*coldata)[i];
                auto block_buffer = block->data();
                auto bit_width = block_buffer[0];
                auto num_entry = ((uint32_t *) (block_buffer + 1))[0];
                auto data_begin = block_buffer + 5;
                ::sboost::encoding::rlehybrid::between(data_begin, bitmap->raw(), counter, bit_width, num_entry,
                                                       rawFrom, rawTo);
            }
            return prev & *bitmap;
        }
    }
}