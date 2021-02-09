//
// Created by harper on 2/2/21.
//

#include "encoding.h"
#include <sboost/byteutils.h>
#include <sboost/unpacker.h>
#include <parquet/encoding.h>
#include <arrow/util/rle_encoding.h>
#include <parquet/platform.h>

namespace lqf {
    namespace encoding {

        template
        class Encoder<parquet::Int32Type>;

        template
        class Decoder<parquet::Int32Type>;

        template
        class Encoder<parquet::DoubleType>;

        template
        class Decoder<parquet::DoubleType>;

        template<typename DT>
        class PlainEncoder : public Encoder<DT> {
            using data_type = typename DT::c_type;
        protected:
            std::vector<data_type> buffer_;
        public:
            virtual void Add(data_type value) override {
                buffer_.push_back(value);
            }

            virtual shared_ptr<vector<shared_ptr<Buffer>>> Dump() override {
                auto result = parquet::AllocateBuffer(default_memory_pool(), sizeof(data_type) * buffer_.size());
                memcpy(result->mutable_data(), buffer_.data(), result->size());

                auto resvec = make_shared<vector<shared_ptr<Buffer>>>();
                resvec->push_back(result);
                return resvec;
            }
        };

        template<typename DT>
        class PlainDecoder : public Decoder<DT> {
            using data_type = typename DT::c_type;
        protected:
            shared_ptr<Buffer> buffer_;
            const data_type *view_;
            uint32_t position_ = 0;
            uint32_t left_ = 0;

        public:
            virtual void SetData(shared_ptr<vector<shared_ptr<Buffer>>> data) override {
                buffer_ = move((*data)[0]);
                view_ = reinterpret_cast<const data_type *>(buffer_->data());
                left_ = buffer_->size() / sizeof(data_type);
            };

            virtual uint32_t Decode(data_type *dest, uint32_t expect) override {
                uint32_t max = expect <= left_ ? expect : left_;
                for (uint32_t i = 0; i < max; ++i) {
                    dest[i] = view_[position_++];
                }
                left_ -= max;
                return max;
            };
        };

        // Due to package access problem of parquet's DictEncoding, we implement the DictEncoder here by ourselves
        // This DictEncoder encode indices in blocks of a fixed size,
        // The last block of the returned buffer is dictionary block
        static uint32_t DICT_BLOCK_SIZE = 4096;

        using namespace parquet;

        template<typename DT>
        class DictEncoder : public Encoder<DT> {
            using data_type = typename DT::c_type;
        protected:
            unordered_map<data_type, int32_t> dictionary_;
            vector<int32_t> indices_;
            vector<shared_ptr<Buffer>> blocks_;

            void DumpBlock() {
                auto bit_width = BitUtil::Log2(dictionary_.size());
                auto block_size = 6 + arrow::util::RleEncoder::MaxBufferSize(
                        bit_width, static_cast<int>(indices_.size())) +
                                  arrow::util::RleEncoder::MinBufferSize(bit_width);
                std::shared_ptr<ResizableBuffer> buffer =
                        AllocateBuffer(default_memory_pool(), block_size);
                auto header = buffer->mutable_data();
                header[0] = bit_width;
                ((uint32_t *) (header + 1))[0] = indices_.size();
                arrow::util::RleEncoder rleEncoder(header + 5, buffer->size(), bit_width);
                for (auto &index:indices_) {
                    rleEncoder.Put(index);
                }
                rleEncoder.Flush();
                blocks_.push_back(buffer);
                indices_.clear();
            }

            void WriteIndex(uint32_t index) {
                indices_.push_back(index);
                if (indices_.size() >= DICT_BLOCK_SIZE) {
                    DumpBlock();
                }
            }

        public:
            DictEncoder() {}

            void Add(data_type value) override {
                auto found = dictionary_.find(value);
                uint32_t index;
                if (found == dictionary_.end()) {
                    index = dictionary_.size();
                    dictionary_[value] = index;
                } else {
                    index = found->second;
                }
                WriteIndex(index);
            }

            shared_ptr<vector<shared_ptr<Buffer>>> Dump() override {
                DumpBlock();
                // Write Dictionary Block
                auto dictblock = AllocateBuffer(default_memory_pool(), sizeof(data_type) * dictionary_.size());
                data_type *view = (data_type *) dictblock->mutable_data();
                for (auto &entry:dictionary_) {
                    view[entry.second] = entry.first;
                }
                blocks_.push_back(dictblock);

                auto ret = make_shared<vector<shared_ptr<Buffer>>>(move(blocks_));
                return ret;
            }
        };

        template<typename DT>
        class DictDecoder : public Decoder<DT> {
            using data_type = typename DT::c_type;
        protected:
            shared_ptr<vector<shared_ptr<Buffer>>> blocks_;
            const data_type *dictionary_;

            unique_ptr<arrow::util::RleDecoder> decoder_ = nullptr;
            uint32_t block_index_ = -1;
            uint32_t block_remain_ = 0;

            int32_t key_buffer_[1024];
        protected:
            bool LoadBlock() {
                block_index_++;
                if (block_index_ >= blocks_->size() - 1) {
                    // The last one is dictionary
                    return false;
                }
                auto buffer = (*blocks_)[block_index_];
                auto bitwidth = buffer->data()[0];
                block_remain_ = ((uint32_t *) (buffer->data() + 1))[0];
                decoder_ = unique_ptr<arrow::util::RleDecoder>(
                        new arrow::util::RleDecoder(buffer->data() + 5, buffer->size() - 5, bitwidth));
                return true;
            }

        public:
            DictDecoder() {}

            virtual void SetData(shared_ptr<vector<shared_ptr<Buffer>>> data) override {
                blocks_ = move(data);
                dictionary_ = reinterpret_cast<const data_type *>((*blocks_)[blocks_->size() - 1]->data());
            }

            virtual uint32_t Decode(data_type *dest, uint32_t expect) override {
                auto data_remain = expect;
                auto write_pos = 0;

                while (data_remain > 0) {
                    if (!decoder_) {
                        auto load = LoadBlock();
                        if (!load)
                            break; // no more block
                    }

                    auto inblock_load = block_remain_ >= data_remain ? data_remain : block_remain_;
                    auto inblock_remain = inblock_load;
                    while (inblock_remain > 0) {
                        auto batch_size = inblock_remain > 1024 ? 1024 : inblock_remain;
                        uint32_t loaded = decoder_->GetBatch(key_buffer_, batch_size);
                        // Translate using dictionary
                        for (uint32_t i = 0; i < loaded; ++i) {
                            dest[write_pos + i] = dictionary_[key_buffer_[i]];
                        }
                        inblock_remain -= loaded;
                        write_pos += loaded;
                    }

                    block_remain_ -= inblock_load;
                    data_remain -= inblock_load;
                    if (block_remain_ == 0) {
                        decoder_ = nullptr;
                    }
                }

                return expect - data_remain;
            }
        };

        static int BP_BLOCK_SIZE = 4096;

        class BitpackEncoder : Encoder<parquet::Int32Type> {
        protected:
            vector<shared_ptr<Buffer>> blocks_;
            vector<int32_t> buffer_;

            void DumpBlock() {
                auto min = INT32_MAX;
                auto max = INT32_MIN;
                for (auto &item: buffer_) {
                    min = item < min ? item : min;
                    max = item > max ? item : max;
                }
                auto diff = max - min;
                auto bit_width = parquet::BitUtil::Log2(diff);

                for (uint32_t i = 0; i < buffer_.size(); ++i) {
                    buffer_[i] -= min;
                }

                auto real_num_entry = buffer_.size();
                auto num_group = ((buffer_.size() + 7) >> 3);
                auto num_entry = num_group << 3;
                // Filler
                for (int i = 0; i < num_entry - real_num_entry; i++) {
                    buffer_.push_back(0);
                }
                // Bit-pack the delta

                auto size_in_byte = num_group * bit_width;

                size_in_byte += 4; // num_entry;
                size_in_byte += 4; //min_value
                size_in_byte += 1;  // bit_width
                auto block = AllocateBuffer(default_memory_pool(), size_in_byte);
                auto intview = reinterpret_cast<int32_t *>(block->mutable_data());
                intview[0] = real_num_entry;
                intview[1] = min;
                block->mutable_data()[8] = bit_width;

                ::sboost::byteutils::bitpack(reinterpret_cast<uint32_t *>(buffer_.data()), num_entry, bit_width,
                                             block->mutable_data() + 9);

                blocks_.push_back(block);
                buffer_.clear();
            }

        public:
            void Add(int32_t value) override {
                buffer_.push_back(value);
                if (buffer_.size() >= BP_BLOCK_SIZE) {
                    DumpBlock();
                }
            }

            shared_ptr<vector<shared_ptr<Buffer>>> Dump() {
                DumpBlock();

                auto ret = make_shared<vector<shared_ptr<Buffer>>>(move(blocks_));
                return ret;
            }
        };

        class BitpackDecoder : Decoder<parquet::Int32Type> {
        protected:
            shared_ptr<vector<shared_ptr<Buffer>>> data_;

            uint32_t current_block_index_ = -1;
            ::sboost::Unpacker *unpacker_ = nullptr;
            uint32_t block_num_entry_;
            __m256i block_min_;
            uint32_t block_counter_;
            uint32_t block_bit_width_;
            uint8_t *block_pointer_;

            bool LoadNextBlock() {
                current_block_index_++;
                if (current_block_index_ >= data_->size()) {
                    unpacker_ = nullptr;
                    return false;
                }
                auto current_buffer = (*data_)[current_block_index_].get();
                auto raw_data = current_buffer->data();

                auto intview = reinterpret_cast<const int32_t *>(raw_data);
                block_num_entry_ = intview[0];
                block_min_ = _mm256_set1_epi32(intview[1]);
                block_bit_width_ = raw_data[8];
                block_pointer_ = (uint8_t *) (raw_data + 9);

                unpacker_ = ::sboost::unpackers[block_bit_width_];
                block_counter_ = 0;

                return true;
            }

        public:
            void SetData(shared_ptr<vector<shared_ptr<Buffer>>> data) override {
                data_ = move(data);
                LoadNextBlock();
            }

            uint32_t Decode(int32_t *dest, uint32_t expect) override {
                assert(expect == 8);
                if (unpacker_ == nullptr) {
                    return 0;
                }
                auto value = unpacker_->unpack(block_pointer_);
                _mm256_add_epi32(value, block_min_);
                _mm256_store_epi64(reinterpret_cast<void *>(*dest), value);

                block_pointer_ += block_bit_width_;
                if (block_counter_ >= block_num_entry_) {
                    auto loaded = block_num_entry_ % 8;
                    LoadNextBlock();
                    return loaded;
                } else {
                    return 8;
                }
            }
        };

        template<typename DT>
        unique_ptr<Encoder<DT>> GetEncoder(EncodingType type) {
            switch (type) {
                case DICTIONARY:
                    return unique_ptr<Encoder<DT>>(new DictEncoder<DT>());
                case PLAIN:
                    return unique_ptr<Encoder<DT>>(new PlainEncoder<DT>());
                case BITPACK:
                    return unique_ptr<BitpackEncoder>(new BitpackEncoder());
                default:
                    return nullptr;
            }
        }

        template unique_ptr<Encoder<parquet::Int32Type>> GetEncoder(EncodingType);

        template unique_ptr<Encoder<parquet::DoubleType>> GetEncoder(EncodingType);

        template<typename DT>
        unique_ptr<Decoder<DT>> GetDecoder(EncodingType type) {
            switch (type) {
                case DICTIONARY:
                    return unique_ptr<Decoder<DT>>(new DictDecoder<DT>());
                case PLAIN:
                    return unique_ptr<Decoder<DT>>(new PlainDecoder<DT>());
                default:
                    return nullptr;
            }
        }

        template unique_ptr<Decoder<parquet::Int32Type>> GetDecoder(EncodingType);

        template unique_ptr<Decoder<parquet::DoubleType>> GetDecoder(EncodingType);
    }
}