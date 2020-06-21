//
// Created by Harper on 4/20/20.
//

#include "container.h"
#include <iostream>

namespace lqf {

    namespace container {

        uint32_t ceil2(uint32_t input) {
            if (__builtin_popcount(input) == 1) {
                return input;
            }
            return 1 << (32 - __builtin_clz(input));
        }

        bool PhaseConcurrentIntHashMap::_insert(vector<uint64_t> *content, uint32_t content_len, uint64_t entry) {
            auto data = content;
            auto content_mask = content_len - 1;
            int32_t key = static_cast<int>(entry);
            auto insert_index = knuth_hash(key) & content_mask;

            auto insert_value = entry;

            while (static_cast<int>(insert_value) != -1) {
                auto exist = (*content)[insert_index];

                if (static_cast<int>(exist) == static_cast<int>(insert_value)) {
                    // We do not copy the data here as in a parallel env it is not clear who goes first.
                    // Copying data here may cause a data race and need either a CAS or a lock.
                    // As we can always assume the exist one goes later, not copying the data is also not wrong.
                    return false;
                } else if (static_cast<int>(exist) > static_cast<int>(insert_value)) {
                    insert_index = (insert_index + 1) & content_mask;
                } else if (__atomic_compare_exchange_n(content->data() + insert_index, &exist, insert_value, false,
                                                       __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                    insert_value = exist;
                    insert_index = (insert_index + 1) & content_mask;
                }
            }
            return true;
        }

        pair<uint32_t, uint64_t> PhaseConcurrentIntHashMap::_findreplacement(uint32_t start_index) {
            auto content_mask = content_len_ - 1;
            auto ref_index = start_index;
            uint64_t ref_entry;
            do {
                ++ref_index;
                ref_entry = content_[ref_index & content_mask];
            } while (static_cast<int>(ref_entry) != -1 &&
                     (knuth_hash(static_cast<int>(ref_entry)) & content_mask) > start_index);
            auto back_index = ref_index - 1;
            while (back_index > start_index) {
                auto cand_entry = content_[back_index & content_mask];
                if (static_cast<int>(cand_entry) == -1 ||
                    (knuth_hash(static_cast<int>(cand_entry)) & content_mask) <= start_index) {
                    ref_index = back_index;
                    ref_entry = cand_entry;
                }
                --back_index;
            }
            return pair<uint32_t, uint64_t>(ref_index, ref_entry);
        }

        PhaseConcurrentIntHashMap::PhaseConcurrentIntHashMap() : PhaseConcurrentIntHashMap(1048576) {}

        PhaseConcurrentIntHashMap::PhaseConcurrentIntHashMap(uint32_t expect_size) : size_(0) {
            content_len_ = ceil2(expect_size * SCALE);
            content_.resize(content_len_, 0xFFFFFFFFFFFFFFFF);
        }

        void PhaseConcurrentIntHashMap::put(int key, int value) {
            uint64_t entry = (static_cast<uint64_t>(value) << 32) | key;
            if (_insert(&content_, content_len_, entry)) {
                size_++;
                if (size_ >= limit()) {
                    // TODO What error?
                    throw std::invalid_argument("hash map is full");
                }
            }
        }

        int PhaseConcurrentIntHashMap::get(int key) {
            auto content_size = content_len_;
            auto content_mask = content_size - 1;
            auto index = knuth_hash(key) & content_mask;
            while (static_cast<int>(content_[index]) != -1 && static_cast<int>(content_[index]) != key) {
                index = (index + 1) & content_mask;
            }
            if (static_cast<int>(content_[index]) == -1) {
                return INT32_MIN;
            }
            return static_cast<int>(content_[index] >> 32);
        }

        int PhaseConcurrentIntHashMap::remove(int key) {
            auto content_size = content_len_;
            auto content_mask = content_size - 1;
            int base_index = knuth_hash(key) & content_mask;
            int ref_index = base_index;
            uint64_t ref_entry = key;
            int retval = INT32_MIN;
            bool cas_success = false;
            while (static_cast<int>(content_[ref_index & content_mask]) != -1 &&
                   key < static_cast<int>(content_[ref_index & content_mask])) {
                ref_index += 1;
            }
            while (ref_index >= base_index) {
                if (static_cast<int>(ref_entry) == -1 ||
                    static_cast<int>(ref_entry) != static_cast<int>(content_[ref_index & content_mask])) {
                    ref_index -= 1;
                } else {
                    int ref_key = static_cast<int>(ref_entry);
                    ref_entry = content_[ref_index & content_mask];
                    ref_entry &= 0xFFFFFFFF00000000;
                    ref_entry |= ref_key;
                    auto cand = _findreplacement(ref_index);
                    auto cand_index = cand.first;
                    auto cand_entry = cand.second;
                    if (__atomic_compare_exchange_n(content_.data() + (ref_index & content_mask),
                                                    &ref_entry, cand_entry, true, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                        cas_success = true;
                        if (retval == INT32_MIN) {
                            retval = static_cast<int>(ref_entry >> 32);
                        }
                        if (static_cast<int>(cand_entry) != -1) {
                            base_index = knuth_hash(static_cast<int>(cand_entry)) & content_mask;
                            ref_index = cand_index;
                            ref_entry = cand_entry;
                        } else {
                            if (cas_success) {
                                size_--;
                            }
                            return retval;
                        }
                    } else {
                        ref_index -= 1;
                    }
                }
            }
            if (cas_success) {
                size_--;
            }
            return retval;
        }

        void PhaseConcurrentIntHashMap::resize(uint64_t expect) {
            auto new_size = ceil2(expect);

            auto new_content_len = ceil2(new_size);
            auto new_content = vector<uint64_t>(new_content_len);

            // Rehash all elements
            for (uint32_t i = 0; i < content_len_; ++i) {
                auto entry = content_[i];
                if (static_cast<int>(entry) != -1) {
                    _insert(&new_content, new_content_len, entry);
                }
            }
            content_.clear();
            content_ = move(new_content);
            content_len_ = new_content_len;
        }

        class PCIHMIterator : public Iterator<pair<int, int>> {
            vector<uint64_t> &content_;
            uint32_t content_len_;
            uint32_t pointer_;
        public:
            PCIHMIterator(vector<uint64_t> &content, uint32_t content_len)
                    : content_(content), content_len_(content_len), pointer_(0) {
                while (pointer_ < content_len_ && static_cast<int>(content_[pointer_]) == -1) {
                    ++pointer_;
                }
            }

            virtual ~PCIHMIterator() = default;

            bool hasNext() {
                return pointer_ < content_len_;
            }

            pair<int, int> next() {
                uint64_t value = content_[pointer_];
                do { ++pointer_; } while (pointer_ < content_len_ && static_cast<int>(content_[pointer_]) == -1);
                return pair<int, int>{static_cast<int>(value), static_cast<int>(value >> 32)};
            }
        };

        unique_ptr<Iterator<pair<int, int>>> PhaseConcurrentIntHashMap::iterator() {
            return unique_ptr<Iterator<pair<int, int>>>(new PCIHMIterator(content_, content_len_));
        }
    }
}