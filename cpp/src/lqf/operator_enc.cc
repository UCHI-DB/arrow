//
// Created by harper on 2/3/21.
//

#include "operator_enc.h"
#include <sboost/unpacker.h>
#include <immintrin.h>

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
            for (uint32_t i = 0; i < coldata->size() - 1; ++i) {
                auto buffer_block = (*coldata)[i];
                auto block_buffer = buffer_block->data();
                auto bit_width = block_buffer[0];
                auto num_entry = ((uint32_t *) (block_buffer + 1))[0];
                auto data_begin = block_buffer + 5;
                ::sboost::encoding::rlehybrid::between(data_begin, bitmap->raw(), counter, bit_width, num_entry,
                                                       rawFrom, rawTo);
            }
            return prev & *bitmap;
        }

        EncMatBetweenFilter::EncMatBetweenFilter(initializer_list<parquet::Type::type> types,
                                                 initializer_list<encoding::EncodingType> encs,
                                                 uint32_t col_index, int32_t lower, int32_t upper)
                : data_types_(types), encodings_(encs), col_index_(col_index), lower_val_(lower), upper_val_(upper) {
            buffer_ = (int32_t *) aligned_alloc(64, sizeof(int32_t) * 16);
        }

        EncMatBetweenFilter::~EncMatBetweenFilter() noexcept {
            free(buffer_);
        }

        shared_ptr<Block> EncMatBetweenFilter::processBlock(const shared_ptr<Block> &block) {
            auto encblock = (EncMemvBlock *) block.get();

            auto filtercol = encblock->rawcol(col_index_);

            auto outputblock = make_shared<EncMemvBlock>(data_types_, encodings_);

            auto num_col = data_types_.size();
            unique_ptr<ColumnIterator> readers[num_col];
            for (uint32_t i = 0; i < num_col; ++i) {
                if (i != col_index_) {
                    readers[i] = encblock->col(i);
                }
            }

            auto writer = outputblock->rows();

            auto lower_512 = _mm512_set1_epi32(lower_val_);
            auto upper_512 = _mm512_set1_epi32(upper_val_);
            auto total_count = 0;

            for (auto &block_buffer: *filtercol) {
                auto block_buffer_header = block_buffer->mutable_data();
                auto intview = (int32_t *) block_buffer_header;
                auto num_entry = intview[0];
                auto min = _mm256_set1_epi32(intview[1]);
                auto bit_width = block_buffer_header[8];
                auto bitpack_start = block_buffer_header + 9;
                auto unpacker = ::sboost::unpackers[bit_width];
                for (int item_counter = 0; item_counter < num_entry; item_counter += 16) {
                    auto loaded1 = _mm256_add_epi32(min, unpacker->unpack(bitpack_start));
                    bitpack_start += bit_width;
                    auto loaded2 = _mm256_add_epi32(min, unpacker->unpack(bitpack_start));
                    bitpack_start += bit_width;

                    auto loaded = _mm512_castsi256_si512(loaded1);
                    loaded = _mm512_inserti32x8(loaded, loaded2, 1);

                    auto le = _mm512_cmp_epi32_mask(loaded, upper_512, _MM_CMPINT_LE);
                    auto ge = _mm512_cmp_epi32_mask(loaded, lower_512, _MM_CMPINT_NLT);
                    auto res = le & ge;

                    auto item_start = item_counter;
                    if (item_counter + 16 > num_entry) {
                        auto remain = num_entry - item_counter;
                        res &= (1 << remain) - 1;
                        total_count += remain;
                    } else {
                        total_count += 16;
                    }

                    // Collect the value
                    Bitset ite(res);

                    while (ite.hasNext()) {
                        auto next = ite.next();
                        DataRow &row = writer->next();

                        for (uint32_t j = 0; j < num_col; ++j) {
                            if (j != col_index_) {
                                row[j] = (*readers[j])[item_start + next];
                            } else {
                                auto value = (int32_t) ((loaded[next / 2] >> ((next % 2) * 32)) & 0xFFFFFFFF);
                                row[j] = value;
                            }
                        }

                    }
                }
            }

            writer->close();
            return outputblock;
        }
    }
}