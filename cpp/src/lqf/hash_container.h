//
// Created by Harper on 4/29/20.
//

#ifndef ARROW_HASH_CONTAINER_H
#define ARROW_HASH_CONTAINER_H

#include "container.h"
#include "data_model.h"

namespace lqf {

    using namespace container;

    namespace hashcontainer {

        template<typename DTYPE>
        class IntPredicate {
            using ktype = typename DTYPE::type;
        public:
            virtual bool test(ktype) = 0;
        };

        using Int32Predicate = IntPredicate<Int32>;
        using Int64Predicate = IntPredicate<Int64>;

        template<typename DTYPE>
        class HashPredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            PhaseConcurrentHashSet<DTYPE> content_;
            atomic<ktype> min_;
            atomic<ktype> max_;
        public:
            HashPredicate();

            void add(ktype);

            bool test(ktype) override;
        };

        using Hash32Predicate = HashPredicate<Int32>;
        using Hash64Predicate = HashPredicate<Int64>;

        class BitmapPredicate : public Int32Predicate {
        private:
            SimpleBitmap bitmap_;
        public:
            BitmapPredicate(uint32_t max);

            void add(int32_t);

            bool test(int32_t) override;
        };

        template<typename DTYPE>
        class HashContainer : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        protected:
            PhaseConcurrentHashMap<DTYPE, MemDataRow *> hashmap_;
            atomic<ktype> min_;
            atomic<ktype> max_;

        public:
            HashContainer();

            void add(ktype key, unique_ptr<MemDataRow> dataRow);

            bool test(ktype) override;

            MemDataRow *get(ktype key);

            unique_ptr<MemDataRow> remove(ktype key);

            unique_ptr<lqf::Iterator<std::pair<ktype, MemDataRow *>>> iterator();

            inline uint32_t size() { return hashmap_.size(); }

            inline ktype min() { return min_.load(); }

            inline ktype max() { return max_.load(); }
        };

        using Hash32Container = HashContainer<Int32>;
        using Hash64Container = HashContainer<Int64>;

        template<typename CONTENT>
        class HashMemBlock : public MemBlock {
        private:
            shared_ptr<CONTENT> content_;
        public:
            HashMemBlock(shared_ptr<CONTENT> predicate);

            shared_ptr<CONTENT> content();
        };

        class HashBuilder {
        public:
            static shared_ptr<Int32Predicate> buildHashPredicate(Table &input, uint32_t);

            static shared_ptr<Int64Predicate> buildHashPredicate(Table &input, function<int64_t(DataRow &)>);

            static shared_ptr<Int32Predicate> buildBitmapPredicate(Table &input, uint32_t);

            static shared_ptr<Hash32Container>
            buildContainer(Table &input, uint32_t, function<unique_ptr<MemDataRow>(DataRow &)>);

            static shared_ptr<Hash64Container>
            buildContainer(Table &input, function<int64_t(DataRow &)>, function<unique_ptr<MemDataRow>(DataRow &)>);
        };

    }
}


#endif //ARROW_HASH_CONTAINER_H
