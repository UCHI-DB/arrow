//
// Created by harper on 2/5/20.
//

#include <assert.h>
#include <cstring>
#include <algorithm>
#include <immintrin.h>
#include "validate.h"
#include "bitmap.h"
#include <sboost/simd.h>

namespace lqf {

    Bitset::Bitset(uint64_t value) : value_(value) {}

    uint32_t Bitset::size() {
        return __builtin_popcount(value_);
    }

    uint32_t Bitset::next() {
        uint64_t t = value_ & -value_; // isolate rightmost 1
        uint32_t result = __builtin_popcount(t - 1);
        value_ ^= t;
        return result;
    }

    bool Bitset::hasNext() {
        return value_ != 0;
    }

    static uint64_t MINUS_ONE = 0xFFFFFFFFFFFFFFFFL;

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

    void SimpleBitmapIterator::moveTo(uint64_t pos) {
        pointer_ = pos >> 6;
        // Find next non-zero pointer
        if (content_[pointer_] != 0) {
            uint64_t remain = pos & 0x3F;
            cached_ = content_[pointer_];
            cached_ &= 0xFFFFFFFFFFFFFFFFL << remain;
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

    SimpleBitmapInvIterator::SimpleBitmapInvIterator(uint64_t *content, uint64_t content_size, uint64_t num_bits) {
        this->content_ = content;
        // The unused part of bitmap is filled with 0, need to exclude them
        this->content_size_ = (num_bits + 63) >> 6;
        this->num_bits_ = num_bits;
        this->final_mask_ = ~((1L << (num_bits & 0x3f)) - 1);

        for (pointer_ = 0; pointer_ < content_size_; ++pointer_) {
            if ((cached_ = content_[pointer_]) != MINUS_ONE) {
                break;
            }
        }
    }

    void SimpleBitmapInvIterator::moveTo(uint64_t pos) {
        pointer_ = pos >> 6;
        // Find next non-all-one pointer
        if (content_[pointer_] != MINUS_ONE) {
            uint64_t remain = pos & 0x3F;
            cached_ = content_[pointer_];
            cached_ |= MINUS_ONE << remain;
            while (cached_ == MINUS_ONE) {
                ++pointer_;
                if (pointer_ == content_size_) {
                    break;
                }
                cached_ = content_[pointer_];
            }
        } else {
            while (content_[pointer_] == MINUS_ONE && pointer_ < content_size_) {
                pointer_++;
            }
            if (pointer_ < content_size_) {
                cached_ = content_[pointer_];
            }
        }
    }

    bool SimpleBitmapInvIterator::hasNext() {
        return pointer_ < content_size_ - 1 ||
               (pointer_ == content_size_ - 1 && (cached_ | final_mask_) != MINUS_ONE);
    }

    uint64_t SimpleBitmapInvIterator::next() {
        uint64_t t = (cached_ | (cached_ + 1)) ^cached_; // isolate rightmost 0
        uint64_t answer = (pointer_ << 6) + _mm_popcnt_u64(t - 1);
        cached_ |= t;
        while (cached_ == MINUS_ONE) {
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
        // Attention: Due to a glitch in sboost, the bitmap should be one word larger than
        // the theoretical size. Otherwise sboost will read past the boundary and cause
        // memory issues.
        // In addition, the RLE encoding may have at most 503 entries appended to the tail.
        // For simplicity, we just make the bitmap large enough.
        array_size_ = (size >> 6) + 10;
        bitmap_ = (uint64_t *) aligned_alloc(64, sizeof(uint64_t) * array_size_);
        memset(bitmap_, 0, sizeof(uint64_t) * array_size_);
        size_ = (int) size;
    }

    SimpleBitmap::SimpleBitmap(SimpleBitmap &&move) {
        array_size_ = move.array_size_;
        size_ = move.size_;
        first_valid_ = move.first_valid_;
        bitmap_ = move.bitmap_;
//        cached_cardinality_ = move.cached_cardinality_;
//        dirty_ = move.dirty_;

        move.bitmap_ = nullptr;
    }

    SimpleBitmap::~SimpleBitmap() {
        if (bitmap_ != nullptr)
            free(bitmap_);
        bitmap_ = nullptr;
    }

    SimpleBitmap &SimpleBitmap::operator=(SimpleBitmap &&move) {
        if (bitmap_ != nullptr)
            free(bitmap_);
        array_size_ = move.array_size_;
        size_ = move.size_;
        first_valid_ = move.first_valid_;
        bitmap_ = move.bitmap_;
//        cached_cardinality_ = move.cached_cardinality_;
//        dirty_ = move.dirty_;

        move.bitmap_ = nullptr;
        return *this;
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
//        dirty_ = true;
    }

    void SimpleBitmap::clear() {
        memset(bitmap_, 0, sizeof(uint64_t) * array_size_);
//        cached_cardinality_ = 0;
//        dirty_ = false;
    }

    shared_ptr<Bitmap> SimpleBitmap::operator&(Bitmap &another) {
        SimpleBitmap &sx1 = static_cast<SimpleBitmap &>(another);
        assert(size_ == sx1.size_);
        this->first_valid_ = -1;
        sboost::simd::simd_and(bitmap_, sx1.bitmap_, array_size_);
//        dirty_ = true;
        return shared_from_this();
    }

    shared_ptr<Bitmap> SimpleBitmap::operator|(Bitmap &another) {
        SimpleBitmap &sx1 = static_cast<SimpleBitmap &>(another);
        assert(size_ == sx1.size_);
        this->first_valid_ = -1;
        sboost::simd::simd_or(bitmap_, sx1.bitmap_, array_size_);
//        dirty_ = true;
        return shared_from_this();
    }

    shared_ptr<Bitmap> SimpleBitmap::operator^(Bitmap &another) {
        SimpleBitmap &sx1 = static_cast<SimpleBitmap &>(another);
        assert(size_ == sx1.size_);
//        validate_true(size_ == sx1.size_, "size not the same");
        this->first_valid_ = -1;
        uint64_t limit = (array_size_ >> 3) << 3;
        uint64_t i = 0;
        for (i = 0; i < limit; i += 8) {
            __m512i a = _mm512_load_si512((__m512i * )(this->bitmap_ + i));
            __m512i b = _mm512_load_si512((__m512i * )(sx1.bitmap_ + i));
            __m512i res = _mm512_xor_si512(a, b);
            _mm512_store_si512((__m512i * )(this->bitmap_ + i), res);
        }
        for (; i < array_size_; ++i) {
            this->bitmap_[i] ^= sx1.bitmap_[i];
        }
//        dirty_ = true;
        return shared_from_this();
    }

    shared_ptr<Bitmap> SimpleBitmap::operator~() {
        this->first_valid_ = -1;
        uint64_t limit = (array_size_ >> 3) << 3;
        uint64_t i = 0;
        __m512i ONE = _mm512_set1_epi64(-1);
        for (i = 0; i < limit; i += 8) {
            __m512i a = _mm512_load_si512((__m512i * )(this->bitmap_ + i));
            __m512i res = _mm512_xor_si512(a, ONE);
            _mm512_store_si512((__m512i * )(this->bitmap_ + i), res);
        }
        for (; i < array_size_; ++i) {
            this->bitmap_[i] ^= -1;
        }
//        dirty_ = true;
        return shared_from_this();
    }

    uint64_t SimpleBitmap::cardinality() {
//        if (dirty_) {
        uint64_t counter = 0;
        uint64_t limit = size_ / 64;
        uint64_t offset = size_ & 0x3F;
        for (uint64_t i = 0; i < limit; i++) {
            counter += _mm_popcnt_u64(bitmap_[i]);
        }
        if (offset > 0) {
            auto last = bitmap_[limit] & ((1L << offset) - 1);
            counter += _mm_popcnt_u64(last);
        }
        return counter;
//            cached_cardinality_ = counter;
//            dirty_ = false;
//        }
//        return cached_cardinality_;
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

    std::unique_ptr<BitmapIterator> SimpleBitmap::inv_iterator() {
        return std::unique_ptr<BitmapIterator>(
                new SimpleBitmapInvIterator(this->bitmap_, this->array_size_, this->size_));
    }

    shared_ptr<Bitmap> SimpleBitmap::mask(Bitmap &input) {
        auto ite = iterator();
        auto input_zeroite = input.inv_iterator();
        auto ite_size = input.size();

        assert(!input.isFull());
        auto next_flip = input_zeroite->next();
        for (uint64_t i = 0; i < ite_size; ++i) {
            if (i < next_flip) {
                ite->next();
            } else {
                erase(ite->next());
                next_flip = input_zeroite->next();
            }
        }
//        dirty_ = true;
        return shared_from_this();
    }

    void SimpleBitmap::erase(uint64_t pos) {
        uint32_t index = static_cast<uint32_t>(pos >> 6);
        uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
        bitmap_[index] &= ~(1L << offset);
//        dirty_ = true;
    }


    uint64_t *SimpleBitmap::raw() {
        return bitmap_;
    }

    ConcurrentBitmap::ConcurrentBitmap(uint64_t
                                       size) : SimpleBitmap(size) {}

    void ConcurrentBitmap::put(uint64_t pos) {
        uint32_t index = static_cast<uint32_t>(pos >> 6);
        uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
        uint64_t modify = 1L << offset;
        assert(index < array_size_);
        uint64_t oldval = bitmap_[index];
        uint64_t newval;
        do {
            newval = oldval | modify;
        } while (!__atomic_compare_exchange_n(bitmap_ + index, &oldval, newval, false,
                                              std::memory_order_seq_cst, std::memory_order_seq_cst));
    }

    uint32_t RleBitmap::_search(uint64_t target) {
        if (data_.empty()) {
            return -1;
        }
        if (data_[0].first > target) {
            return -1;
        }
        if (data_[data_.size() - 1].first < target) {
            return -(data_.size() + 1) - 1;
        }
        uint32_t left = 0; // item before left is less than target
        uint32_t right = data_.size() - 1; // item after right is greater than target

        while (left <= right) {
            auto middle = (left + right + 1) / 2;
            auto val = data_[middle].first;

            if (val == target) {
                return middle;
            }
            if (val > target) {
                right = middle - 1;
            } else {
                left = middle;
            }
        }
        return -left - 1;
    }

    RleBitmap::RleBitmap(uint64_t size) : size_(size) {}

    bool RleBitmap::check(uint64_t pos) {
        auto index = _search(pos);
        if (index >= 0) {
            return true;
        } else {
            auto lessthan = -index - 1;
            if (lessthan == 0) {
                return false;
            }
            if (lessthan == data_.size()) {
                lessthan -= 1;
            }
            auto entry = data_[lessthan];
            return entry.first + entry.second > pos;
        }
    }

    void RleBitmap::put(uint64_t pos) {
        if (data_.empty()) {
            data_.push_back(pair(pos, 1));
            return;
        }
        auto last = data_[data_.size() - 1];
        // Fast insert path for continuous insert
        if (last.first + last.second == pos) {
            last.second++;
            return;
        } else if (pos > last.first + last.second) {
            data_.push_back(pair(pos, 1));
            return;
        }
        // Look for position
        // TODO If we want to do better we should check if the new pos merges two existing RLE entry. Not doing it now
        auto index = _search(pos);
        if (index < 0) { // do nothing for >=0 as it already exists
            auto lessthan = -index - 1;
            if (lessthan == 0) {
                data_.emplace(data_.begin(), pos, 1);
                return;
            }
            auto entry = data_[lessthan - 1];
            auto boundary = entry.first + entry.second;
            if (boundary == pos) {
                entry.second++;
            }
            if (boundary < pos) {
                // need new entry
                data_.emplace(data_.begin() + lessthan + 1, pair<uint64_t, uint32_t>(pos, 1));
            }
        }
    }

    void RleBitmap::clear() {
        data_.clear();
    }

    uint64_t RleBitmap::cardinality() {
        uint64_t sum = 0L;
        for (auto &data: data_) {
            sum += data.second;
        }
        return sum;
    }

    uint64_t RleBitmap::size() {
        return size_;
    }

    bool RleBitmap::isFull() {
        // TODO Beware of overflow
        return data_[0].first == 0 && data_[0].second == size_;
    }

    bool RleBitmap::isEmpty() {
        return data_.empty();
    }

    double RleBitmap::ratio() {
        return ((double) cardinality()) / size();
    }

    shared_ptr<Bitmap> RleBitmap::operator&(Bitmap &x1) {
        throw "not implemented";
    }

    shared_ptr<Bitmap> RleBitmap::operator^(Bitmap &x1) {
        throw "not implemented";
    }

    shared_ptr<Bitmap> RleBitmap::operator|(Bitmap &x1) {
        throw "not implemented";
    }

    shared_ptr<Bitmap> RleBitmap::operator~() {
        throw "not implemented";
    }

    std::unique_ptr<BitmapIterator> RleBitmap::iterator() {
        throw "not implemented";
    }

    std::unique_ptr<BitmapIterator> RleBitmap::inv_iterator() {
        throw "not implemented";
    }

    shared_ptr<Bitmap> RleBitmap::mask(Bitmap &) {
        throw "not implemented";
    }

    FullBitmap::FullBitmap(uint64_t size) {
        this->size_ = size;
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

    shared_ptr<Bitmap> FullBitmap::operator&(Bitmap &x1) {
        return x1.shared_from_this();
    }

    shared_ptr<Bitmap> FullBitmap::operator|(Bitmap &x1) {
        return shared_from_this();
    }

    shared_ptr<Bitmap> FullBitmap::operator^(Bitmap &x1) {
        return ~x1;
    }

    shared_ptr<Bitmap> FullBitmap::operator~() {
        return make_shared<SimpleBitmap>(size_);
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

    std::unique_ptr<BitmapIterator> FullBitmap::inv_iterator() {
        return std::unique_ptr<BitmapIterator>(new EmptyBitmapIterator());
    }

    shared_ptr<Bitmap> FullBitmap::mask(Bitmap &input) {
        return input.shared_from_this();
    }

}
