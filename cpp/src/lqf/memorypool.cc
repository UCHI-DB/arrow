//
// Created by Harper on 6/4/20.
//

#include "memorypool.h"
#include <cstring>

namespace lqf {
    namespace memory {

        thread_local uint32_t ByteArrayBuffer::index_ = -1;
        thread_local uint32_t ByteArrayBuffer::offset_ = SLAB_SIZE;

        ByteArrayBuffer ByteArrayBuffer::instance;

        ByteArrayBuffer::ByteArrayBuffer() {}

        ByteArrayBuffer::~ByteArrayBuffer() {
            for (auto &slab:buffer_) {
                free(slab);
            }
        }

        void ByteArrayBuffer::new_slab() {
            uint8_t *new_buffer = (uint8_t *) malloc(SLAB_SIZE);

            std::lock_guard<mutex> lock(assign_lock_);
            buffer_.push_back(new_buffer);
            index_ = buffer_.size() - 1;
            offset_ = 0;
        }

        void ByteArrayBuffer::allocate(parquet::ByteArray &input) {
            if (offset_ + input.len > SLAB_SIZE) {
                new_slab();
            }
            uint8_t *target = buffer_[index_] + offset_;
            std::memcpy(target, input.ptr, input.len);
            input.ptr = target;
            offset_ += input.len;
        }
    }
}