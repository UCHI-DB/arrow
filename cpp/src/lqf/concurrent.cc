//
// Created by Harper on 4/20/20.
//

#include "concurrent.h"
#include <iostream>
#include <exception>

namespace lqf {

    const int32_t Int32::empty = -1;
    const int32_t Int32::min = INT32_MIN;
    const int32_t Int32::max = INT32_MAX;

    const int64_t Int64::empty = -1;
    const int64_t Int64::min = INT64_MIN;
    const int64_t Int64::max = INT64_MAX;

    namespace phasecon {

        uint32_t ceil2(uint32_t input) {
            if (__builtin_popcount(input) == 1) {
                return input;
            }
            return 1 << (32 - __builtin_clz(input));
        }


        template<typename DTYPE>
        PhaseConcurrentHashSet<DTYPE>::PhaseConcurrentHashSet() : PhaseConcurrentHashSet(1048576) {}

        template<typename DTYPE>
        PhaseConcurrentHashSet<DTYPE>::PhaseConcurrentHashSet(uint32_t expect_size) : size_(0) {
            content_ = new vector<type>(ceil2(expect_size * SCALE), DTYPE::empty);
        }

        template<typename DTYPE>
        PhaseConcurrentHashSet<DTYPE>::~PhaseConcurrentHashSet() {
            delete content_;
        }

        template<typename DTYPE>
        bool PhaseConcurrentHashSet<DTYPE>::_insert(vector<type> *content, type value) {
            auto data = content->data();
            auto content_size = content_->size();
            auto content_mask = content_size - 1;
            auto insert_index = knuth_hash(value) & content_mask;
            auto insert_value = value;
            while (insert_value != DTYPE::empty) {
                auto exist = (*content)[insert_index];
                if (exist == insert_value) {
                    return 0;
                } else if (exist > insert_value) {
                    insert_index = (insert_index + 1) & content_mask;
                } else if (__sync_bool_compare_and_swap(data + insert_index, exist, insert_value)) {
                    insert_value = exist;
                    insert_index = (insert_index + 1) & content_mask;
                }
            }
            return 1;
        }

        template<typename DTYPE>
        pair<uint32_t, typename DTYPE::type>
        PhaseConcurrentHashSet<DTYPE>::_findreplacement(uint32_t start_index) {
            auto content_mask = content_->size() - 1;
            auto ref_index = start_index;
            auto data = content_->data();
            type ref_value;
            do {
                ++ref_index;
                ref_value = data[ref_index & content_mask];
            } while (ref_value != DTYPE::empty && (knuth_hash(ref_value) & content_mask) > start_index);
            auto back_index = ref_index - 1;
            while (back_index > start_index) {
                auto cand_value = data[back_index & content_mask];
                if (cand_value == DTYPE::empty || (knuth_hash(cand_value) & content_mask) <= start_index) {
                    ref_value = cand_value;
                    ref_index = back_index;
                }
                --back_index;
            }
            return pair<uint32_t, type>(ref_index, ref_value);
        }

        template<typename DTYPE>
        void PhaseConcurrentHashSet<DTYPE>::add(type value) {
            if (_insert(content_, value)) {
                size_++;
                if (size_ >= limit()) {
                    // TODO What error?
                    throw std::invalid_argument("hash set is full");
                }
            }
            // resize operation cannot be done here.
            // Will race with insert
//            if (size_ * SCALE > content_->size()) {
//                internal_resize(content_->size() << 1);
//            }
        }

        template<typename DTYPE>
        bool PhaseConcurrentHashSet<DTYPE>::test(type value) {
            auto content_size = content_->size();
            auto content_mask = content_size - 1;
            auto index = knuth_hash(value) & content_mask;
            while ((*content_)[index] != DTYPE::empty && (*content_)[index] != value) {
                index = (index + 1) & content_mask;
            }
            return (*content_)[index] == value;
        }

        template<typename DTYPE>
        void PhaseConcurrentHashSet<DTYPE>::remove(type value) {
            auto data = content_->data();
            auto content_mask = content_->size() - 1;
            auto start_index = knuth_hash(value) & content_mask;
            auto ref_index = start_index;
            auto ref_value = value;
            while (data[ref_index & content_mask] != DTYPE::empty
                   && data[ref_index & content_mask] > ref_value) {
                ++ref_index;
            }
            while (ref_index >= start_index) {
                if (ref_value == DTYPE::empty || data[ref_index & content_mask] != ref_value) {
                    --ref_index;
                } else {
                    auto cand = _findreplacement(ref_index);
                    auto cand_index = cand.first;
                    auto cand_value = cand.second;
                    auto data_index = ref_index & content_mask;
                    if (__sync_bool_compare_and_swap(data + data_index, ref_value, cand_value)) {
                        if (cand_value != DTYPE::empty) {
                            ref_index = cand_index;
                            ref_value = cand_value;
                            start_index = knuth_hash(ref_value) & content_mask;
                        } else {
                            return;
                        }
                    } else {
                        --ref_index;
                    }
                }
            }
        }

        // A cost operation requiring re-hashing of all values
        template<typename DTYPE>
        void PhaseConcurrentHashSet<DTYPE>::resize(uint32_t expect) {
            auto new_size = ceil2(expect);
            if (content_->size() < new_size) {
                auto new_content = new vector<type>(new_size, DTYPE::empty);
                // Rehash all elements
                for (auto &value: *content_) {
                    if (value != DTYPE::empty) {
                        _insert(new_content, value);
                    }
                }

                delete content_;
                content_ = new_content;
            }
        }

        template
        class PhaseConcurrentHashSet<Int32>;

        template
        class PhaseConcurrentHashSet<Int64>;

    }
}