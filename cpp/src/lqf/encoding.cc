//
// Created by harper on 2/2/21.
//

#include "encoding.h"
#include <parquet/encoding.h>
#include <arrow/util/rle_encoding.h>
#include <parquet/platform.h>

namespace lqf {
    namespace encoding {

        // Due to package access problem of parquet's DictEncoding, we implement the DictEncoder here by ourselves
        // This DictEncoder encode indices in blocks of a fixed size,
        // The last block of the returned buffer is dictionary block
        static uint32_t DICT_BLOCK_SIZE = 4096;

        using namespace parquet;

        class DictEncoder : public Encoder {
        protected:
            unordered_map<int32_t, int32_t> dictionary_;
            vector<int32_t> indices_;
            vector<shared_ptr<Buffer>> blocks_;

            void DumpBlock() {
                auto bit_width = BitUtil::Log2(indices_.size());
                auto block_size = 2 + arrow::util::RleEncoder::MaxBufferSize(
                        bit_width, static_cast<int>(indices_.size())) +
                                  arrow::util::RleEncoder::MinBufferSize(bit_width);
                std::shared_ptr<ResizableBuffer> buffer =
                        AllocateBuffer(default_memory_pool(), block_size);
                auto header = buffer->mutable_data();
                header[0] = bit_width;
                arrow::util::RleEncoder rleEncoder(header + 1, buffer->size(), bit_width);
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

            void Add(int32_t value) override {
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
                auto dictblock = AllocateBuffer(default_memory_pool(), sizeof(int32_t) * dictionary_.size());
                int32_t *view = (int32_t *) dictblock->mutable_data();
                for (auto &entry:dictionary_) {
                    view[entry.second] = entry.first;
                }
                blocks_.push_back(dictblock);

                auto ret = make_shared<vector<shared_ptr<Buffer>>>(move(blocks_));
                return ret;
            }
        };

        class DictDecoder : public Decoder {
        protected:
            shared_ptr<vector<shared_ptr<Buffer>>> blocks_;
            const int32_t *dictionary_;

            unique_ptr<arrow::util::RleDecoder> decoder_ = nullptr;
            uint32_t block_index_ = -1;
        protected:
            bool LoadBlock() {
                block_index_++;
                if (block_index_ >= blocks_->size() - 1) {
                    // The last one is dictionary
                    return false;
                }
                auto buffer = (*blocks_)[block_index_];
                auto bitwidth = buffer->data()[0];
                decoder_ = unique_ptr<arrow::util::RleDecoder>(
                        new arrow::util::RleDecoder(buffer->data() + 1, buffer->size() - 1, bitwidth));
                return true;
            }

        public:
            DictDecoder() {}

            virtual void SetData(shared_ptr<vector<shared_ptr<Buffer>>> data) override {
                blocks_ = move(data);
                dictionary_ = reinterpret_cast<const int32_t *>((*blocks_)[blocks_->size() - 1]->data());
            }

            virtual uint32_t Decode(int32_t *dest, uint32_t expect) override {
                if (!decoder_) {
                    auto load = LoadBlock();
                    if (!load)
                        return 0;
                }
                uint32_t loaded = decoder_->GetBatch(dest, expect);
                // Translate using dictionary
                for (auto i = 0; i < loaded; ++i) {
                    dest[i] = dictionary_[dest[i]];
                }
                if (loaded < expect) {
                    decoder_ = nullptr;
                    return loaded + Decode(dest + loaded, expect - loaded);
                }
                return loaded;
            }
        };

        unique_ptr<Encoder> GetEncoder(Type type) {
            switch (type) {
                case DICTIONARY:
                    return unique_ptr<Encoder>(new DictEncoder());
                default:
                    return nullptr;
            }
        }

        unique_ptr<Decoder> GetDecoder(Type type) {
            switch (type) {
                case DICTIONARY:
                    return unique_ptr<Decoder>(new DictDecoder());
                default:
                    return nullptr;
            }
        }
    }
}