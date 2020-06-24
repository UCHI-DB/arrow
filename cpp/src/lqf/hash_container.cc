//
// Created by Harper on 4/29/20.
//

#include "hash_container.h"

namespace lqf {
    namespace hashcontainer {

        template<typename DTYPE>
        HashPredicate<DTYPE>::HashPredicate():min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE>
        HashPredicate<DTYPE>::HashPredicate(uint32_t size):content_(size), min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE>
        void HashPredicate<DTYPE>::add(ktype val) {
            // Update range
            ktype current;
            current = min_.load();
            while (val < current && !min_.compare_exchange_strong(current, val));

            current = max_.load();
            while (val > current && !max_.compare_exchange_strong(current, val));
            content_.add(val);
        }

        template<typename DTYPE>
        bool HashPredicate<DTYPE>::test(ktype val) {
            if (val >= min_ && val <= max_) {
                return content_.test(val);
            }
            return false;
        }

        template
        class HashPredicate<Int32>;

        template
        class HashPredicate<Int64>;

        BitmapPredicate::BitmapPredicate(uint32_t size) : bitmap_(size) {}

        void BitmapPredicate::add(int32_t val) {
            bitmap_.put(val);
        }

        bool BitmapPredicate::test(int32_t val) {
            return bitmap_.check(val);
        }

        template<typename DTYPE, typename MAP>
        HashContainer<DTYPE, MAP>::HashContainer(const vector<uint32_t> &offset)
                :HashContainer(offset, 1048576) {}

        template<typename DTYPE, typename MAP>
        HashContainer<DTYPE, MAP>::HashContainer(const vector<uint32_t> &offset, uint32_t size)
                : map_(size, offset), min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE, typename MAP>
        DataRow &HashContainer<DTYPE, MAP>::add(ktype key) {
            ktype current;

            current = min_.load();
            while (key < current && !min_.compare_exchange_strong(current, key));

            current = max_.load();
            while (key > current && !max_.compare_exchange_strong(current, key));
            return map_.insert(key);
        }

        template<typename DTYPE, typename MAP>
        DataRow *HashContainer<DTYPE, MAP>::get(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return map_.find(key);
        }

        template<typename DTYPE, typename MAP>
        DataRow *HashContainer<DTYPE, MAP>::remove(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return map_.remove(key);
        }

        template<typename DTYPE, typename MAP>
        bool HashContainer<DTYPE, MAP>::test(ktype key) {
            return map_.find(key) != nullptr;
        }

        template<typename DTYPE, typename MAP>
        unique_ptr<Iterator<pair<typename DTYPE::type, DataRow &> &>> HashContainer<DTYPE, MAP>::iterator() {
            return map_.map_iterator();
        }

        template
        class HashContainer<Int32, CInt32MemRowMap>;

        template
        class HashContainer<Int64, CInt64MemRowMap>;

        template<typename CONTENT>
        HashMemBlock<CONTENT>::HashMemBlock(shared_ptr<CONTENT> predicate) : MemBlock(0, 0) {
            content_ = move(predicate);
        }

        template<typename CONTENT>
        shared_ptr<CONTENT> HashMemBlock<CONTENT>::content() {
            return content_;
        }

        template
        class HashMemBlock<Int32Predicate>;

        template
        class HashMemBlock<Int64Predicate>;

        template
        class HashMemBlock<Hash32Predicate>;

        template
        class HashMemBlock<Hash64Predicate>;

        template
        class HashMemBlock<Hash32Container>;

        template
        class HashMemBlock<Hash64Container>;

        shared_ptr<Int32Predicate>
        HashBuilder::buildHashPredicate(Table &input, uint32_t keyIndex, uint32_t expect_size) {
            Hash32Predicate *pred = new Hash32Predicate(expect_size);
            shared_ptr<Int32Predicate> retval = shared_ptr<Int32Predicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, keyIndex](const shared_ptr<Block> &block) {
                        auto hashpredblock = dynamic_pointer_cast<HashMemBlock<Int32Predicate>>(block);
                        if (hashpredblock) {
                            retval = hashpredblock->content();
                            return;
                        } else {
                            auto hashcontblock = dynamic_pointer_cast<HashMemBlock<Hash32Container>>(block);
                            if (hashcontblock) {
                                retval = hashcontblock->content();
                                return;
                            }
                        }
                        auto col = block->col(keyIndex);
                        uint32_t block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            pred->add(col->next().asInt());
                        }
                    };
            input.blocks()->foreach(processor);
            return retval;
        }

        shared_ptr<Int64Predicate>
        HashBuilder::buildHashPredicate(Table &input, function<int64_t(DataRow &)> key_maker) {
            Hash64Predicate *pred = new Hash64Predicate();
            shared_ptr<Int64Predicate> retval = shared_ptr<Int64Predicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, key_maker](const shared_ptr<Block> &block) {
                        auto hashblock = dynamic_pointer_cast<HashMemBlock<Int64Predicate>>(block);
                        if (hashblock) {
                            retval = hashblock->content();
                            return;
                        } else {
                            auto hashcblock = dynamic_pointer_cast<HashMemBlock<Hash64Container>>(block);
                            if (hashcblock) {
                                retval = hashcblock->content();
                                return;
                            }
                        }
                        auto rows = block->rows();
                        auto block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            DataRow &row = rows->next();
                            pred->add(key_maker(row));
                        }
                    };
            input.blocks()->foreach(processor);
            return retval;
        }

        shared_ptr<Int32Predicate>
        HashBuilder::buildBitmapPredicate(Table &input, uint32_t keyIndex, uint32_t expect_size) {
            auto predicate = new BitmapPredicate(expect_size);
            function<void(const shared_ptr<Block> &)> processor = [=](const shared_ptr<Block> &block) {
                auto col = block->col(keyIndex);
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    auto key = col->next().asInt();
                    predicate->add(key);
                }
            };
            input.blocks()->foreach(processor);
            return shared_ptr<Int32Predicate>(predicate);
        }

        shared_ptr<Hash32Container> HashBuilder::buildContainer(Table &input, uint32_t keyIndex,
                                                                Snapshoter *builder, uint32_t expect_size) {
            Hash32Container *container = new Hash32Container(builder->colOffset(), expect_size);
            shared_ptr<Hash32Container> retval = shared_ptr<Hash32Container>(container);

            function<void(const shared_ptr<Block> &)> processor = [builder, keyIndex, &container, &retval](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock<Hash32Container>>(block);
                if (hashblock) {
                    retval = hashblock->content();
                    return;
                }
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    auto key = row[keyIndex].asInt();
                    DataRow &writeto = container->add(key);
                    (*builder)(writeto, row);
                }
            };
            input.blocks()->foreach(processor);
            return retval;
        }

        shared_ptr<Hash64Container> HashBuilder::buildContainer(Table &input,
                                                                function<int64_t(DataRow &)> key_maker,
                                                                Snapshoter *builder, uint32_t expect_size) {
            Hash64Container *container = new Hash64Container(builder->colOffset(), expect_size);
            shared_ptr<Hash64Container> retval = shared_ptr<Hash64Container>(container);
            function<void(const shared_ptr<Block> &)> processor = [builder, &container, &retval, key_maker](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock<Hash64Container>>(block);
                if (hashblock) {
                    retval = hashblock->content();
                }
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    auto key = key_maker(row);
                    DataRow &writeto = container->add(key);
                    (*builder)(writeto, row);
                }
            };
            input.blocks()->foreach(processor);
            return retval;
        }

    }
}