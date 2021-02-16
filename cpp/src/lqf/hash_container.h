//
// Created by Harper on 4/29/20.
//

#ifndef ARROW_HASH_CONTAINER_H
#define ARROW_HASH_CONTAINER_H

#include <cuckoohash_map.hh>
#include <sparsehash/dense_hash_set>
#include "container.h"
#include "data_model.h"
#include "rowcopy.h"
#include "data_container.h"

#define CONTAINER_SIZE 1048576

namespace lqf {

    using namespace container;
    using namespace rowcopy;

    namespace hashcontainer {

        template<typename DTYPE>
        class IntPredicate {
            using ktype = typename DTYPE::type;
        public:
            virtual ~IntPredicate() = default;

            virtual bool test(ktype) = 0;
        };

        using Int32Predicate = IntPredicate<Int32>;
        using Int64Predicate = IntPredicate<Int64>;

        template<typename DTYPE>
        class HashPredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            PhaseConcurrentHashSet<DTYPE> content_;
        public:
            HashPredicate();

            HashPredicate(uint32_t size);

            virtual ~HashPredicate() = default;

            void add(ktype);

            bool test(ktype) override;

            inline uint32_t size() { return content_.size(); }

        };

        using Hash32Predicate = HashPredicate<Int32>;
        using Hash64Predicate = HashPredicate<Int64>;

        template<typename DTYPE>
        class HashSetPredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            unordered_set<ktype> content_;
        public:
            HashSetPredicate();

            HashSetPredicate(uint32_t size);

            virtual ~HashSetPredicate() = default;

            void add(ktype);

            bool test(ktype) override;

            inline uint32_t size() { return content_.size(); }
        };

        using Hash32SetPredicate = HashSetPredicate<Int32>;
        using Hash64SetPredicate = HashSetPredicate<Int64>;

        template<typename DTYPE>
        class HashGooglePredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            google::dense_hash_set<ktype> content_;
        public:
            HashGooglePredicate() : HashGooglePredicate(CONTAINER_SIZE) {}

            HashGooglePredicate(uint32_t size) {
                content_.set_empty_key(-1);
            }

            virtual ~HashGooglePredicate() = default;

            void add(ktype value) {
                content_.insert(value);
            }

            bool test(ktype value) override {
                return content_.find(value) != content_.end();
            }

            inline uint32_t size() { return content_.size(); }
        };

        using Hash32GooglePredicate = HashGooglePredicate<Int32>;
        using Hash64GooglePredicate = HashGooglePredicate<Int64>;

        template<typename DTYPE>
        class HashCuckooPredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            libcuckoo::cuckoohash_map<ktype, ktype> content_;
        public:
            HashCuckooPredicate() {}

            HashCuckooPredicate(uint32_t size) {}

            virtual ~HashCuckooPredicate() = default;

            void add(ktype value) {
                content_.insert(value, value);
            }

            bool test(ktype value) override {
                return content_.contains(value);
            }

            inline uint32_t size() { return content_.size(); }
        };

        using Hash32CuckooPredicate = HashCuckooPredicate<Int32>;
        using Hash64CuckooPredicate = HashCuckooPredicate<Int64>;

        class BitmapPredicate : public Int32Predicate {
        private:
            ConcurrentBitmap bitmap_;
        public:
            BitmapPredicate(uint32_t max);

            virtual ~BitmapPredicate() = default;

            void add(int32_t);

            bool test(int32_t) override;
        };

        using namespace datacontainer;

        /**
         * PhaseConcurrentMap based heap allocation MemDataRow container
         * @tparam DTYPE
         */
        template<typename DTYPE>
        class HashSparseContainer : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        protected:
            vector<uint32_t> col_offset_;
            PhaseConcurrentHashMap<DTYPE, MemDataRow *> map_;
            atomic<ktype> min_;
            atomic<ktype> max_;

        public:
            HashSparseContainer(const vector<uint32_t> &);

            HashSparseContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~HashSparseContainer() = default;

            DataRow &add(ktype key);

            bool test(ktype) override;

            DataRow *get(ktype key);

            unique_ptr<DataRow> remove(ktype key);

            unique_ptr<lqf::Iterator<std::pair<ktype, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline ktype min() { return min_.load(); }

            inline ktype max() { return max_.load(); }
        };

        using Hash32SparseContainer = HashSparseContainer<Int32>;
        using Hash64SparseContainer = HashSparseContainer<Int64>;

        /**
         * PhaseConcurrentMap based page allocation container
         * @tparam DTYPE
         * @tparam MAP
         */
        template<typename DTYPE, typename MAP>
        class HashDenseContainer : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        protected:
            MAP map_;
            atomic<ktype> min_;
            atomic<ktype> max_;

        public:
            HashDenseContainer(const vector<uint32_t> &);

            HashDenseContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~HashDenseContainer() = default;

            DataRow &add(ktype key);

            bool test(ktype) override;

            DataRow *get(ktype key);

            DataRow *remove(ktype key);

            unique_ptr<lqf::Iterator<std::pair<ktype, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline ktype min() { return min_.load(); }

            inline ktype max() { return max_.load(); }
        };

        using Hash32DenseContainer = HashDenseContainer<Int32, CInt32MemRowMap>;
        using Hash64DenseContainer = HashDenseContainer<Int64, CInt64MemRowMap>;

        /**
         * A 32-bit map based heap allocation container
         */
        class Hash32MapHeapContainer : public IntPredicate<Int32> {
        protected:
            vector<uint32_t> col_offset_;
            google::dense_hash_map<int32_t, MemDataRow *> map_;
            atomic<int32_t> min_;
            atomic<int32_t> max_;

        public:
            Hash32MapHeapContainer(const vector<uint32_t> &);

            Hash32MapHeapContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~Hash32MapHeapContainer() noexcept;

            DataRow &add(int32_t key);

            bool test(int32_t) override;

            DataRow *get(int32_t key);

            unique_ptr<DataRow> remove(int32_t key);

            unique_ptr<lqf::Iterator<std::pair<int32_t, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline int32_t min() { return min_.load(); }

            inline int32_t max() { return max_.load(); }
        };

        /**
         * A 32-bit map based page allocation container
         */
        class Hash32MapPageContainer : public IntPredicate<Int32> {
        protected:
            MemRowMap map_;
            atomic<int32_t> min_;
            atomic<int32_t> max_;

        public:
            Hash32MapPageContainer(const vector<uint32_t> &);

            Hash32MapPageContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~Hash32MapPageContainer() = default;

            DataRow &add(int32_t key);

            bool test(int32_t) override;

            DataRow *get(int32_t key);

            unique_ptr<DataRow> remove(int32_t key);

            unique_ptr<lqf::Iterator<std::pair<int32_t, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline int32_t min() { return min_.load(); }

            inline int32_t max() { return max_.load(); }
        };

        /**
          * A 32-bit cuckoo map based heap allocation container
          */
        class Hash32CuckooHeapContainer : public IntPredicate<Int32> {
        protected:
            vector<uint32_t> col_offset_;
            libcuckoo::cuckoohash_map<int32_t, shared_ptr<MemDataRow>> map_;
            atomic<int32_t> min_;
            atomic<int32_t> max_;

        public:
            Hash32CuckooHeapContainer(const vector<uint32_t> &);

            Hash32CuckooHeapContainer(const vector<uint32_t> &, uint32_t size);

            virtual ~Hash32CuckooHeapContainer() noexcept;

            DataRow &add(int32_t key);

            bool test(int32_t) override;

            DataRow *get(int32_t key);

            unique_ptr<DataRow> remove(int32_t key);

            unique_ptr<lqf::Iterator<std::pair<int32_t, DataRow &> &>> iterator();

            inline uint32_t size() { return map_.size(); }

            inline int32_t min() { return min_.load(); }

            inline int32_t max() { return max_.load(); }
        };


        using Hash32Container = Hash32SparseContainer;
        using Hash64Container = Hash64SparseContainer;

        using namespace datacontainer;

        template<typename CONTENT>
        class HashMemBlock : public MemBlock {
        private:
            shared_ptr<CONTENT> content_;
        public:
            HashMemBlock(shared_ptr<CONTENT> predicate);

            virtual ~HashMemBlock() = default;

            shared_ptr<CONTENT> content();
        };

        class HashBuilder {
        public:
            static shared_ptr<Int32Predicate>
            buildHashPredicate(Table &input, uint32_t, uint32_t expect_size = CONTAINER_SIZE);

            static shared_ptr<Int64Predicate> buildHashPredicate(Table &input, function<int64_t(DataRow &)>);

            static shared_ptr<Int32Predicate> buildBitmapPredicate(Table &input, uint32_t, uint32_t);

            static shared_ptr<Hash32Container>
            buildContainer(Table &input, uint32_t, Snapshoter *, uint32_t expect_size = CONTAINER_SIZE);

            static shared_ptr<Hash64Container>
            buildContainer(Table &input, function<int64_t(DataRow &)>, Snapshoter *,
                           uint32_t expect_size = CONTAINER_SIZE);
        };

        class PredicateBuilder {
        public:
            template<typename P32>
            static shared_ptr<P32> build(Table &input, uint32_t, uint32_t expect_size = CONTAINER_SIZE) {
                return nullptr;
            }
        };

        // Specializations
        template<>
        shared_ptr<Hash32Predicate> PredicateBuilder::build<Hash32Predicate>(Table &, uint32_t, uint32_t);

        template<>
        shared_ptr<Hash32SetPredicate> PredicateBuilder::build<Hash32SetPredicate>(Table &, uint32_t, uint32_t);

        // Specializations
        template<>
        shared_ptr<Hash32CuckooPredicate> PredicateBuilder::build<Hash32CuckooPredicate>(Table &, uint32_t, uint32_t);

        template<>
        shared_ptr<Hash32GooglePredicate> PredicateBuilder::build<Hash32GooglePredicate>(Table &, uint32_t, uint32_t);


        // TODO Use this to replace the one in HashBuilder
        class ContainerBuilder {
        public:
            template<typename C32>
            static shared_ptr<C32> build(Table &, uint32_t, Snapshoter *, uint32_t expect_size = CONTAINER_SIZE) {
                // Default implementation return nothing
                return nullptr;
            }

            template<typename C64>
            static shared_ptr<C64> build(Table &,
                                         function<int64_t(DataRow &)>, Snapshoter *,
                                         uint32_t expect_size = CONTAINER_SIZE) {
                // Default implementation return nothing
                return nullptr;
            }
        };

        // Specializations
        template<>
        shared_ptr<Hash32SparseContainer>
        ContainerBuilder::build<Hash32SparseContainer>(Table &input, uint32_t keyIndex,
                                                       Snapshoter *builder, uint32_t expect_size);

        template<>
        shared_ptr<Hash64SparseContainer>
        ContainerBuilder::build<Hash64SparseContainer>(Table &input,
                                                       function<int64_t(DataRow &)> key_maker,
                                                       Snapshoter *builder, uint32_t expect_size);

        template<>
        shared_ptr<Hash32DenseContainer>
        ContainerBuilder::build<Hash32DenseContainer>(Table &input, uint32_t keyIndex,
                                                      Snapshoter *builder, uint32_t expect_size);

        template<>
        shared_ptr<Hash64DenseContainer>
        ContainerBuilder::build<Hash64DenseContainer>(Table &input,
                                                      function<int64_t(DataRow &)> key_maker,
                                                      Snapshoter *builder, uint32_t expect_size);

        template<>
        shared_ptr<Hash32MapHeapContainer>
        ContainerBuilder::build<Hash32MapHeapContainer>(Table &input, uint32_t keyIndex,
                                                        Snapshoter *builder, uint32_t expect_size);

        template<>
        shared_ptr<Hash32MapPageContainer>
        ContainerBuilder::build<Hash32MapPageContainer>(Table &input, uint32_t keyIndex,
                                                        Snapshoter *builder, uint32_t expect_size);

        template<>
        shared_ptr<Hash32CuckooHeapContainer>
        ContainerBuilder::build<Hash32CuckooHeapContainer>(Table &input, uint32_t keyIndex,
                                                           Snapshoter *builder, uint32_t expect_size);

    }
}


#endif //ARROW_HASH_CONTAINER_H
