//
// Created by harper on 2/5/20.
//

#include <arrow/util/logging.h>
#include <cstring>
#include <immintrin.h>
#include "validate.h"
#include "bitmap.h"

namespace chidata {
    namespace util {
        BitmapIterator::~BitmapIterator() {

        }

        SimpleBitmapIterator::SimpleBitmapIterator(uint64_t *content, uint64_t content_size, uint64_t num_bits) {
            this->content_ = content;
            this->content_size_ = content_size;
            this->num_bits_ = num_bits;
            this->final_mask_ = (1L << (num_bits & 0x3f)) - 1;

            for (pointer_ = 0; pointer_ < content_size_; ++pointer_) {
                if ((cached_ = content_[pointer_]) != 0) {
                    break;
                }
            }
        }

        SimpleBitmapIterator::~SimpleBitmapIterator() {

        }

        void SimpleBitmapIterator::moveTo(uint64_t pos) {
            pointer_ = pos >> 6;
            // Find next non-zero pointer
            if (content_[pointer_] != 0) {
                uint64_t remain = pos & 0x3F;
                cached_ = content_[pointer_];
                cached_ &= -1L << remain;
                while (cached_ == 0) {
                    ++pointer_;
                    if (pointer_ == content_size_) {
                        break;
                    }
                    cached_ = content_[pointer_];
                }
            } else {
                while (content_[pointer_] == 0 && pointer_ < content_size_) {
                    pointer_++;
                }
                if (pointer_ < content_size_) {
                    cached_ = content_[pointer_];
                }
            }
        }

        bool SimpleBitmapIterator::hasNext() {
            return pointer_ < content_size_ - 1 ||
                   (pointer_ == content_size_ - 1 && (cached_ & final_mask_) != 0);
        }

        uint64_t SimpleBitmapIterator::next() {
            uint64_t t = cached_ & -cached_; // isolate rightmost 1
            uint64_t answer = (pointer_ << 6) + _mm_popcnt_u64(t - 1);
            cached_ ^= t;
            while (cached_ == 0) {
                ++pointer_;
                if (pointer_ == content_size_) {
                    break;
                }
                cached_ = content_[pointer_];
            }
            return answer;
        }

        FullBitmapIterator::FullBitmapIterator(uint64_t size) {
            this->size_ = size;
            this->counter_ = 0;
        }

        FullBitmapIterator::~FullBitmapIterator() {

        }

        void FullBitmapIterator::moveTo(uint64_t pos) {
            this->counter_ = pos;
        }

        bool FullBitmapIterator::hasNext() {
            return this->counter_ < size_;
        }

        uint64_t FullBitmapIterator::next() {
            return this->counter_++;
        }

        SimpleBitmap::SimpleBitmap(uint64_t size) {
            validate_true(size < 0xFFFFFFFFL);
            array_size_ = (size >> 6) + 1;
            bitmap_ = new uint64_t[array_size_];
            memset(bitmap_, 0, sizeof(uint64_t) * array_size_);
            size_ = (int) size;
        }

        SimpleBitmap::~SimpleBitmap() {
            delete[] bitmap_;
        }

        bool SimpleBitmap::check(uint64_t pos) {
            uint32_t index = static_cast<uint32_t> (pos >> 6);
            uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
            return (bitmap_[index] & (1L << offset)) != 0;
        }

        void SimpleBitmap::put(uint64_t pos) {
            uint32_t index = static_cast<uint32_t>(pos >> 6);
            uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
            bitmap_[index] |= 1L << offset;
        }

        void SimpleBitmap::clear() {
            memset(bitmap_, 0, sizeof(uint64_t) * array_size_);
        }

        Bitmap *SimpleBitmap::land(Bitmap *another) {
            SimpleBitmap *sx1 = static_cast<SimpleBitmap *>(another);
            validate_true(size_ == sx1->size_);
            this->first_valid_ = -1;
            uint64_t limit = array_size_ >> 2;
            uint64_t i = 0;
            for (i = 0; i < limit; i += 4) {
                __m256i a = _mm256_loadu_si256((__m256i *) (this->bitmap_ + i));
                __m256i b = _mm256_loadu_si256((__m256i *) (sx1->bitmap_ + i));
                __m256i res = _mm256_and_si256(a, b);
                _mm256_storeu_si256((__m256i *) (this->bitmap_ + i), res);
            }
            for (; i < array_size_; i++) {
                this->bitmap_[i] &= sx1->bitmap_[i];
            }
            return this;
        }

        Bitmap *SimpleBitmap::lor(Bitmap *another) {
            SimpleBitmap *sx1 = static_cast<SimpleBitmap *>(another);
            validate_true(size_ == sx1->size_);
            this->first_valid_ = -1;
            uint64_t limit = array_size_ >> 2;
            uint64_t i = 0;
            for (i = 0; i < limit; i += 4) {
                __m256i a = _mm256_loadu_si256((__m256i *) (this->bitmap_ + i));
                __m256i b = _mm256_loadu_si256((__m256i *) (sx1->bitmap_ + i));
                __m256i res = _mm256_or_si256(a, b);
                _mm256_storeu_si256((__m256i *) (this->bitmap_ + i), res);
            }
            for (; i < array_size_; i++) {
                this->bitmap_[i] |= sx1->bitmap_[i];
            }
            return this;
        }

        uint64_t SimpleBitmap::cardinality() {
            uint64_t counter = 0;
            for (uint64_t i = 0; i < array_size_; i++) {
                counter += _mm_popcnt_u64(bitmap_[i]);
            }
            return counter;
        }

        uint64_t SimpleBitmap::size() {
            return size_;
        }

        bool SimpleBitmap::isFull() {
            return size_ == cardinality();
        }

        bool SimpleBitmap::isEmpty() {
            return cardinality() == 0;
        }

        double SimpleBitmap::ratio() {
            return (double) cardinality() / size_;
        }

        std::unique_ptr<BitmapIterator> SimpleBitmap::iterator() {
            return std::unique_ptr<BitmapIterator>(
                    new SimpleBitmapIterator(this->bitmap_, this->array_size_, this->size_));
        }

        FullBitmap::FullBitmap(uint64_t size) {
            this->size_ = size;
        }

        FullBitmap::~FullBitmap() {

        }

        bool FullBitmap::check(uint64_t pos) {
            return true;
        }

        void FullBitmap::put(uint64_t pos) {
            throw std::invalid_argument("");
        }

        void FullBitmap::clear() {
            throw std::invalid_argument("");
        }

        Bitmap *FullBitmap::land(Bitmap *x1) {
            return x1;
        }

        Bitmap *FullBitmap::lor(Bitmap *x1) {
            return this;
        }

        uint64_t FullBitmap::cardinality() {
            return size_;
        }

        uint64_t FullBitmap::size() {
            return size_;
        }

        bool FullBitmap::isFull() {
            return true;
        }

        bool FullBitmap::isEmpty() {
            return false;
        }

        double FullBitmap::ratio() {
            return 1;
        }

        std::unique_ptr<BitmapIterator> FullBitmap::iterator() {
            return std::unique_ptr<BitmapIterator>(new FullBitmapIterator(this->size_));
        }
    }
}
