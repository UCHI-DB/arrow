//
// Created by Harper on 4/29/20.
//

#include "hash_container.h"

namespace lqf {
    namespace hashcontainer {

        template<typename DTYPE>
        HashPredicate<DTYPE>::HashPredicate() {}

        template<typename DTYPE>
        HashPredicate<DTYPE>::HashPredicate(uint32_t size):content_(size) {}

        template<typename DTYPE>
        void HashPredicate<DTYPE>::add(ktype val) {
            // Update range
//            ktype current;
//            current = min_.load();
//            while (val < current && !min_.compare_exchange_strong(current, val));
//
//            current = max_.load();
//            while (val > current && !max_.compare_exchange_strong(current, val));
            content_.add(val);
        }

        template<typename DTYPE>
        bool HashPredicate<DTYPE>::test(ktype val) {
            return content_.test(val);
        }

        template
        class HashPredicate<Int32>;

        template
        class HashPredicate<Int64>;

        template<typename DTYPE>
        HashSetPredicate<DTYPE>::HashSetPredicate() {}

        template<typename DTYPE>
        HashSetPredicate<DTYPE>::HashSetPredicate(uint32_t size):content_(size) {}

        template<typename DTYPE>
        void HashSetPredicate<DTYPE>::add(ktype val) {
            // Update range
            content_.insert(val);
        }

        template<typename DTYPE>
        bool HashSetPredicate<DTYPE>::test(ktype val) {
            return content_.find(val) != content_.end();
        }

        template
        class HashSetPredicate<Int32>;

        template
        class HashSetPredicate<Int64>;


        BitmapPredicate::BitmapPredicate(uint32_t size) : bitmap_(size) {}

        void BitmapPredicate::add(int32_t val) {
            bitmap_.put(val);
        }

        bool BitmapPredicate::test(int32_t val) {
            return bitmap_.check(val);
        }

        template<typename DTYPE, typename MAP>
        HashDenseContainer<DTYPE, MAP>::HashDenseContainer(const vector<uint32_t> &offset)
                :HashDenseContainer(offset, 1048576) {}

        template<typename DTYPE, typename MAP>
        HashDenseContainer<DTYPE, MAP>::HashDenseContainer(const vector<uint32_t> &offset, uint32_t size)
                : map_(size, offset), min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE, typename MAP>
        DataRow &HashDenseContainer<DTYPE, MAP>::add(ktype key) {
            ktype current;

            current = min_.load();
            while (key < current && !min_.compare_exchange_strong(current, key));

            current = max_.load();
            while (key > current && !max_.compare_exchange_strong(current, key));
            return map_.insert(key);
        }

        template<typename DTYPE, typename MAP>
        DataRow *HashDenseContainer<DTYPE, MAP>::get(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return map_.find(key);
        }

        template<typename DTYPE, typename MAP>
        DataRow *HashDenseContainer<DTYPE, MAP>::remove(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return map_.remove(key);
        }

        template<typename DTYPE, typename MAP>
        bool HashDenseContainer<DTYPE, MAP>::test(ktype key) {
            if (key > max_ || key < min_)
                return false;
            return map_.test(key);
        }

        template<typename DTYPE, typename MAP>
        unique_ptr<Iterator<pair<typename DTYPE::type, DataRow &> &>> HashDenseContainer<DTYPE, MAP>::iterator() {
            return map_.map_iterator();
        }

        template
        class HashDenseContainer<Int32, CInt32MemRowMap>;

        template
        class HashDenseContainer<Int64, CInt64MemRowMap>;

        template<typename DTYPE>
        HashSparseContainer<DTYPE>::HashSparseContainer(const vector<uint32_t> &offset)
                :HashSparseContainer(offset, 1048576) {}

        template<typename DTYPE>
        HashSparseContainer<DTYPE>::HashSparseContainer(const vector<uint32_t> &offset, uint32_t expect_size)
                : col_offset_(offset), map_(expect_size), min_(DTYPE::max), max_(DTYPE::min) {}

        template<typename DTYPE>
        DataRow &HashSparseContainer<DTYPE>::add(ktype key) {
            ktype current;
            current = min_.load();
            while (key < current && !min_.compare_exchange_strong(current, key));
            current = max_.load();
            while (key > current && !max_.compare_exchange_strong(current, key));

            auto row = new MemDataRow(col_offset_);
            map_.put(key, row);
            return *row;
        }

        template<typename DTYPE>
        bool HashSparseContainer<DTYPE>::test(ktype key) {
            return map_.get(key) != nullptr;
        }

        template<typename DTYPE>
        DataRow *HashSparseContainer<DTYPE>::get(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return map_.get(key);
        }

        template<typename DTYPE>
        unique_ptr<DataRow> HashSparseContainer<DTYPE>::remove(ktype key) {
            if (key > max_ || key < min_)
                return nullptr;
            return unique_ptr<DataRow>(map_.remove(key));
        }

        template<typename DTYPE>
        class HSCIteratorWrapper : public Iterator<pair<typename DTYPE::type, DataRow &> &> {
            using ktype = typename DTYPE::type;
        protected:
            unique_ptr<Iterator<pair<ktype, MemDataRow *>>> iterator_;
            MemDataRowPointer accessor_;
            pair<ktype, DataRow &> pair_;
        public:
            HSCIteratorWrapper(const vector<uint32_t> &col_offset, unique_ptr<Iterator<pair<ktype, MemDataRow *>>> ite)
                    : iterator_(move(ite)), accessor_(col_offset), pair_{0, accessor_} {}

            bool hasNext() { return iterator_->hasNext(); }

            pair<ktype, DataRow &> &next() {
                auto next = iterator_->next();
                pair_.first = next.first;
                accessor_.raw(next.second->raw());
                return pair_;
            }
        };

        template<typename DTYPE>
        unique_ptr<lqf::Iterator<std::pair<typename DTYPE::type, DataRow &> &>> HashSparseContainer<DTYPE>::iterator() {
            return unique_ptr<Iterator<std::pair<typename DTYPE::type, DataRow &> &>>(
                    new HSCIteratorWrapper<DTYPE>(col_offset_, map_.iterator()));
        }

        template
        class HashSparseContainer<Int32>;

        template
        class HashSparseContainer<Int64>;

        Hash32MapHeapContainer::Hash32MapHeapContainer(const vector<uint32_t> &offset)
                : Hash32MapHeapContainer(offset, CONTAINER_SIZE) {}

        Hash32MapHeapContainer::Hash32MapHeapContainer(const vector<uint32_t> &offset, uint32_t size) :
                col_offset_(offset), map_(size) {
            map_.set_empty_key(-1);
        }

        Hash32MapHeapContainer::~Hash32MapHeapContainer() noexcept {
            for (auto &p:map_) {
                delete p.second;
            }
        }

        DataRow &Hash32MapHeapContainer::add(int32_t key) {
            auto row = new MemDataRow(col_offset_);
            map_[key] = row;
            return *row;
        }

        bool Hash32MapHeapContainer::test(int32_t key) {
            return map_.find(key) != map_.end();
        }

        DataRow *Hash32MapHeapContainer::get(int32_t key) {
            auto found = map_.find(key);
            if (found != map_.end()) {
                return found->second;
            }
            return nullptr;
        }

        unique_ptr<DataRow> Hash32MapHeapContainer::remove(int32_t key) {
            auto found = map_.find(key);
            if (found == map_.end()) {
                return nullptr;
            } else {
                DataRow *pointer = found->second;
                map_.erase(found);
                return unique_ptr<DataRow>(pointer);
            }
        }

        unique_ptr<lqf::Iterator<std::pair<int32_t, DataRow &> &>> Hash32MapHeapContainer::iterator() {
            throw "Not implemented";
        }

        Hash32CuckooHeapContainer::Hash32CuckooHeapContainer(const vector<uint32_t> &offset)
                : Hash32CuckooHeapContainer(offset, CONTAINER_SIZE) {}

        Hash32CuckooHeapContainer::Hash32CuckooHeapContainer(const vector<uint32_t> &offset, uint32_t size) :
                col_offset_(offset), map_(size) {
        }

        Hash32CuckooHeapContainer::~Hash32CuckooHeapContainer() noexcept {
            map_.clear();
        }

        DataRow &Hash32CuckooHeapContainer::add(int32_t key) {
            auto row = make_shared<MemDataRow>(col_offset_);
            map_.insert(key, row);
            return *row;
        }

        bool Hash32CuckooHeapContainer::test(int32_t key) {
            return map_.contains(key);
        }

        DataRow *Hash32CuckooHeapContainer::get(int32_t key) {
            try {
                return map_.find(key).get();
            } catch (...) {
                return nullptr;
            }
        }

        unique_ptr<DataRow> Hash32CuckooHeapContainer::remove(int32_t key) {
            throw "Not implemented";
        }

        unique_ptr<lqf::Iterator<std::pair<int32_t, DataRow &> &>> Hash32CuckooHeapContainer::iterator() {
            throw "Not implemented";
        }

        Hash32MapPageContainer::Hash32MapPageContainer(const vector<uint32_t> &offset)
                : Hash32MapPageContainer(offset, CONTAINER_SIZE) {}

        Hash32MapPageContainer::Hash32MapPageContainer(const vector<uint32_t> &offset, uint32_t size) :
                map_(offset, size) {}

        DataRow &Hash32MapPageContainer::add(int32_t key) {
            return map_.insert(key);
        }

        bool Hash32MapPageContainer::test(int32_t key) {
            return map_.find(key) != nullptr;
        }

        DataRow *Hash32MapPageContainer::get(int32_t key) {
            return map_.find(key);
        }

        unique_ptr<DataRow> Hash32MapPageContainer::remove(int32_t key) {
            throw "Not supported";
        }

        unique_ptr<lqf::Iterator<std::pair<int32_t, DataRow &> &>> Hash32MapPageContainer::iterator() {
            throw "Not supported";
        }


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
        class HashMemBlock<Hash32SparseContainer>;

        template
        class HashMemBlock<Hash64SparseContainer>;

        template
        class HashMemBlock<Hash32DenseContainer>;

        template
        class HashMemBlock<Hash64DenseContainer>;

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


        template<>
        shared_ptr<Hash32Predicate>
        PredicateBuilder::build<Hash32Predicate>(Table &input, uint32_t keyIndex, uint32_t expect_size) {
            Hash32Predicate *pred = new Hash32Predicate(expect_size);
            shared_ptr<Hash32Predicate> retval = shared_ptr<Hash32Predicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, keyIndex](const shared_ptr<Block> &block) {
                        auto col = block->col(keyIndex);
                        uint32_t block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            pred->add(col->next().asInt());
                        }
                    };
            input.blocks()->foreach(processor);
            return retval;
        }

        template<>
        shared_ptr<Hash32SetPredicate>
        PredicateBuilder::build<Hash32SetPredicate>(Table &input, uint32_t keyIndex, uint32_t expect_size) {
            Hash32SetPredicate *pred = new Hash32SetPredicate(expect_size);
            shared_ptr<Hash32SetPredicate> retval = shared_ptr<Hash32SetPredicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, keyIndex](const shared_ptr<Block> &block) {
                        auto col = block->col(keyIndex);
                        uint32_t block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            pred->add(col->next().asInt());
                        }
                    };
            input.blocks()->sequential()->foreach(processor);
            return retval;
        }

        template<>
        shared_ptr<Hash32CuckooPredicate>
        PredicateBuilder::build<Hash32CuckooPredicate>(Table &input, uint32_t keyIndex, uint32_t expect_size) {
            Hash32CuckooPredicate *pred = new Hash32CuckooPredicate(expect_size);
            shared_ptr<Hash32CuckooPredicate> retval = shared_ptr<Hash32CuckooPredicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, keyIndex](const shared_ptr<Block> &block) {
                        auto col = block->col(keyIndex);
                        uint32_t block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            pred->add(col->next().asInt());
                        }
                    };
            input.blocks()->foreach(processor);
            return retval;
        }

        template<>
        shared_ptr<Hash32GooglePredicate>
        PredicateBuilder::build<Hash32GooglePredicate>(Table &input, uint32_t keyIndex, uint32_t expect_size) {
            Hash32GooglePredicate *pred = new Hash32GooglePredicate(expect_size);
            shared_ptr<Hash32GooglePredicate> retval = shared_ptr<Hash32GooglePredicate>(pred);

            function<void(const shared_ptr<Block> &)> processor =
                    [&pred, &retval, keyIndex](const shared_ptr<Block> &block) {
                        auto col = block->col(keyIndex);
                        uint32_t block_size = block->size();
                        for (uint32_t i = 0; i < block_size; ++i) {
                            pred->add(col->next().asInt());
                        }
                    };
            input.blocks()->sequential()->foreach(processor);
            return retval;
        }

        template<typename C32>
        shared_ptr<C32> buildContainerP32(Table &input, uint32_t keyIndex, Snapshoter *builder,
                                          uint32_t expect_size = CONTAINER_SIZE) {
            C32 *container = new C32(builder->colOffset(), expect_size);
            shared_ptr<C32> retval = shared_ptr<C32>(container);

            function<void(const shared_ptr<Block> &)> processor = [builder, keyIndex, &container, &retval](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock<C32>>(block);
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

        template<typename C64>
        shared_ptr<C64> buildContainerP64(Table &input, function<int64_t(DataRow &)> key_maker,
                                          Snapshoter *builder, uint32_t expect_size) {
            C64 *container = new C64(builder->colOffset(), expect_size);
            shared_ptr<C64> retval = shared_ptr<C64>(container);
            function<void(const shared_ptr<Block> &)> processor = [builder, &container, &retval, key_maker](
                    const shared_ptr<Block> &block) {
                auto hashblock = dynamic_pointer_cast<HashMemBlock<C64>>(block);
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

        template<typename C32>
        shared_ptr<C32> buildContainerS32(Table &input, uint32_t keyIndex, Snapshoter *builder,
                                          uint32_t expect_size = CONTAINER_SIZE) {
            C32 *container = new C32(builder->colOffset(), expect_size);
            shared_ptr<C32> retval = shared_ptr<C32>(container);

            function<void(const shared_ptr<Block> &)> processor = [builder, keyIndex, &container, &retval](
                    const shared_ptr<Block> &block) {
                auto rows = block->rows();
                auto block_size = block->size();
                for (uint32_t i = 0; i < block_size; ++i) {
                    DataRow &row = rows->next();
                    auto key = row[keyIndex].asInt();
                    DataRow &writeto = container->add(key);
                    (*builder)(writeto, row);
                }
            };
            input.blocks()->sequential()->foreach(processor);
            return retval;
        }

        void buildContainerS64();

        template<>
        shared_ptr<Hash32SparseContainer>
        ContainerBuilder::build<Hash32SparseContainer>(Table &input, uint32_t keyIndex,
                                                       Snapshoter *builder, uint32_t expect_size) {
            return buildContainerP32<Hash32SparseContainer>(input, keyIndex, builder, expect_size);
        }

        template<>
        shared_ptr<Hash64SparseContainer>
        ContainerBuilder::build<Hash64SparseContainer>(Table &input,
                                                       function<int64_t(DataRow &)> key_maker,
                                                       Snapshoter *builder, uint32_t expect_size) {
            return buildContainerP64<Hash64SparseContainer>(input, key_maker, builder, expect_size);
        }

        template<>
        shared_ptr<Hash32DenseContainer>
        ContainerBuilder::build<Hash32DenseContainer>(Table &input, uint32_t keyIndex,
                                                      Snapshoter *builder, uint32_t expect_size) {
            return buildContainerP32<Hash32DenseContainer>(input, keyIndex, builder, expect_size);
        }

        template<>
        shared_ptr<Hash64DenseContainer>
        ContainerBuilder::build<Hash64DenseContainer>(Table &input,
                                                      function<int64_t(DataRow &)> key_maker,
                                                      Snapshoter *builder, uint32_t expect_size) {
            return buildContainerP64<Hash64DenseContainer>(input, key_maker, builder, expect_size);
        }

        template<>
        shared_ptr<Hash32MapHeapContainer>
        ContainerBuilder::build<Hash32MapHeapContainer>(Table &input, uint32_t keyIndex,
                                                        Snapshoter *builder, uint32_t expect_size) {
            return buildContainerS32<Hash32MapHeapContainer>(input, keyIndex, builder, expect_size);
        }

        template<>
        shared_ptr<Hash32MapPageContainer>
        ContainerBuilder::build<Hash32MapPageContainer>(Table &input, uint32_t keyIndex,
                                                        Snapshoter *builder, uint32_t expect_size) {
            return buildContainerS32<Hash32MapPageContainer>(input, keyIndex, builder, expect_size);
        }

        template<>
        shared_ptr<Hash32CuckooHeapContainer>
        ContainerBuilder::build<Hash32CuckooHeapContainer>(Table &input, uint32_t keyIndex,
                                                           Snapshoter *builder, uint32_t expect_size) {
            return buildContainerP32<Hash32CuckooHeapContainer>(input, keyIndex, builder, expect_size);
        }
    }
}