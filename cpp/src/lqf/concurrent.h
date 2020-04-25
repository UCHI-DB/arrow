//
// Created by Harper on 4/20/20.
//

#ifndef ARROW_CONCURRENT_H
#define ARROW_CONCURRENT_H

#include <cstdint>
#include <vector>
#include <memory>
#include <cstring>
#include <mutex>
#include <fstream>
#include <lqf/hash.h>
#include <atomic>

#define SCALE 1.5

using namespace std;

namespace lqf {
    struct Int32 {
        using type = int32_t;
        static const int32_t empty;
        static const int32_t min;
        static const int32_t max;
    };

    struct Int64 {
        using type = int64_t;
        static const int64_t empty;
        static const int64_t min;
        static const int64_t max;
    };

    namespace phasecon {

        using namespace lqf::hash;

        uint32_t ceil2(uint32_t);

        template<typename DTYPE>
        class PhaseConcurrentHashSet {
            using type = typename DTYPE::type;
        protected:
            std::atomic<uint32_t> size_;

            vector<type> *content_;

            bool _insert(vector<type> *, type);

            pair<uint32_t, type> _findreplacement(uint32_t);

        public:
            PhaseConcurrentHashSet();

            PhaseConcurrentHashSet(uint32_t size);

            virtual ~PhaseConcurrentHashSet();

            void add(type);

            bool test(type);

            void remove(type);

            void resize(uint32_t);

            inline uint32_t size() { return size_; }

            inline uint32_t limit() { return (*content_).size(); }
        };

        inline __uint128_t cas128(volatile __uint128_t *src, __uint128_t cmp, __uint128_t exchange) {
            bool success;
            __asm__ __volatile__ (
            "lock cmpxchg16b %1"
            : "+A" ( cmp ), "+m" ( *src ), "=@ccz"(success)
            : "b" ((uint64_t) exchange), "c" ((uint64_t) (exchange >> 64))
            : "cc"
            );
            return success;
        }

        template<typename KTYPE, typename VTYPEP>
        class PhaseConcurrentHashMap {
            using ktype = typename KTYPE::type;
        protected:
            struct Pair {
                ktype key_;
                VTYPEP value_;
            };

            union Entry {
                Pair pair;
                __uint128_t whole;
            };

            std::atomic<uint32_t> size_;

            Entry *content_;
            uint32_t content_len_;

            bool _insert(Entry *content, Entry entry) {
                auto data = content;
                auto content_size = content_len_;
                auto content_mask = content_size - 1;
                auto insert_index = knuth_hash(entry.pair.key_) & content_mask;

                auto insert_value = entry;

                while (insert_value.pair.key_ != KTYPE::empty) {
                    auto exist = content[insert_index];

                    if (exist.pair.key_ == insert_value.pair.key_) {
                        // We do not copy the data here as in a parallel env it is not clear who goes first.
                        // Copying data here may cause a data race and need either a CAS or a lock.
                        // As we can always assume the exist one goes later, not copying the data is also not wrong.
                        return 0;
                    } else if (exist.pair.key_ > insert_value.pair.key_) {
                        insert_index = (insert_index + 1) & content_mask;
                    } else if (cas128((__uint128_t *) data + insert_index, exist.whole, insert_value.whole)) {
                        insert_value = exist;
                        insert_index = (insert_index + 1) & content_mask;
                    }
                }
                return 1;
            }

            pair<uint32_t, Entry> _findreplacement(uint32_t start_index) {
                auto content_mask = content_len_ - 1;
                auto ref_index = start_index;
                Entry ref_entry;
                do {
                    ++ref_index;
                    ref_entry = content_[ref_index & content_mask];
                } while (ref_entry.pair.key_ != KTYPE::empty &&
                         (knuth_hash(ref_entry.pair.key_) & content_mask) > start_index);
                auto back_index = ref_index - 1;
                while (back_index > start_index) {
                    auto cand_entry = content_[back_index & content_mask];
                    if (cand_entry.pair.key_ == KTYPE::empty ||
                        (knuth_hash(cand_entry.pair.key_) & content_mask) <= start_index) {
                        ref_index = back_index;
                        ref_entry = cand_entry;
                    }
                    --back_index;
                }
                return pair<uint32_t, Entry>(ref_index, ref_entry);
            }

        public:

            PhaseConcurrentHashMap() : PhaseConcurrentHashMap(1048576) {}

            PhaseConcurrentHashMap(uint32_t expect_size) : size_(0) {
                content_len_ = ceil2(expect_size * SCALE);
                content_ = (Entry *) aligned_alloc(16, sizeof(Entry) * content_len_);
                memset(content_, -1, sizeof(Entry) * content_len_);
            }

            virtual~ PhaseConcurrentHashMap() {
                for (uint32_t i = 0; i < content_len_; ++i) {
                    delete (content_ + i);
                }
                free(content_);
            }

            void put(ktype key, VTYPEP value) {
                Entry entry;
                entry.pair = {key, value};
                if (_insert(content_, entry)) {
                    size_++;
                    if (size_ >= limit()) {
                        // TODO What error?
                        throw std::invalid_argument("hash set is full");
                    }
                }
            }

            VTYPEP get(ktype key) {
                auto content_size = content_len_;
                auto content_mask = content_size - 1;
                auto index = knuth_hash(key) & content_mask;
                while (content_[index].pair.key_ != KTYPE::empty && content_[index].pair.key_ != key) {
                    index = (index + 1) & content_mask;
                }
                if (content_[index].pair.key_ == KTYPE::empty) {
                    return nullptr;
                }
                return content_[index].pair.value_;
            }

            VTYPEP remove(ktype key) {
                auto content_size = content_len_;
                auto content_mask = content_size - 1;
                auto base_index = knuth_hash(key) & content_mask;
                auto ref_index = base_index;
                Entry ref_entry;
                ref_entry.pair.key_ = key;
                VTYPEP retval = nullptr;
                while (content_[ref_index & content_mask].key_ != KTYPE::empty &&
                       key < content_[ref_index & content_mask].key_) {
                    ref_index += 1;
                }
                while (ref_index >= base_index) {
                    if (ref_entry.pair.key_ == KTYPE::empty ||
                        ref_entry.pair.key_ != content_[ref_index & content_mask].pair.key_) {
                        ref_index -= 1;
                    } else {
                        ktype ref_key = ref_entry.pair.key_;
                        ref_entry = content_[ref_index & content_mask];
                        ref_entry.pair.key_ = ref_key;
                        auto cand = _findreplacement(ref_index);
                        auto cand_index = cand.first;
                        auto cand_entry = cand.second;
                        if (cas128((__uint128_t *) (content_ + (ref_index & content_mask)),
                                   ref_entry.whole, cand_entry.whole)) {
                            if (retval) {
                                retval = ref_entry.pair.value_;
                            }
                            if (cand_entry.pair.key_ != KTYPE::empty) {
                                base_index = knuth_hash(cand_entry.pair.key_) & content_mask;
                                ref_index = cand_index;
                                ref_entry = cand_entry;
                            } else {
                                return retval;
                            }
                        } else {
                            ref_index -= 1;
                        }
                    }
                }
                return retval;
            }

            void resize(uint64_t expect) {
                auto new_size = ceil2(expect);

                auto new_content_len = ceil2(new_size);
                auto new_content = (Entry *) aligned_alloc(16, sizeof(Entry) * new_content_len);
                memset(content_, -1, sizeof(Entry) * new_content_len);

                // Rehash all elements
                for (uint32_t i = 0; i < content_len_; ++i) {
                    auto entry = content_[i];
                    if (entry.key_ != KTYPE::empty) {
                        internal_insert(new_content, entry);
                    }
                }
                free(content_);
                content_ = new_content;
                content_len_ = new_content_len;
            }

            inline uint32_t size() { return size_; }

            inline uint32_t limit() { return content_len_; }
        };
    }
}
#endif //ARROW_CONCURRENT_H
