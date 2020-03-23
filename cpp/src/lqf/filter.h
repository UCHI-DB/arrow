//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_FILTER_H
#define LQF_OPERATOR_FILTER_H

#include <memory>
#include "data_model.h"
#include "bitmap.h"
#include <sboost/encoding/rlehybrid.h>
#include <sboost/encoding/deltabp.h>

#define ACCESS(type, param)
#define ACCESS2(type, param1, param2)

using namespace std;

namespace lqf {

    class Filter {
    protected:
        shared_ptr<Block> processBlock(const shared_ptr<Block> &);

        virtual shared_ptr<Bitmap> filterBlock(Block &) = 0;

    public:
        virtual shared_ptr<Table> filter(Table &input);
    };

    class ColPredicate {
    protected:
        uint32_t index_;
    public:
        ColPredicate(uint32_t);

        virtual ~ColPredicate();

        inline uint32_t index() { return index_; }

        virtual bool supportBatch() { return false; }

        virtual shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) = 0;
    };

    class SimpleColPredicate : public ColPredicate {
    private:
        function<bool(const DataField &)> predicate_;
    public:
        SimpleColPredicate(uint32_t, function<bool(const DataField &)>);

        ~SimpleColPredicate() {}

        shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) override;
    };

    namespace sboost {

        template<typename DTYPE>
        class SboostPredicate : public ColPredicate {
        public:
            SboostPredicate(uint32_t index, function<unique_ptr<RawAccessor<DTYPE>>()> accbuilder) :
                    ColPredicate(index), builder_(accbuilder) {}

            shared_ptr<Bitmap> filterBlock(Block &block, Bitmap &) override;

            unique_ptr<RawAccessor<DTYPE>> build();

            bool supportBatch() override {
                return true;
            }

        protected:
            function<unique_ptr<RawAccessor<DTYPE>>()> builder_;
        };

        using SBoostInt32Predicate = SboostPredicate<Int32Type>;
        using SBoostDoublePredicate = SboostPredicate<DoubleType>;
        using SBoostByteArrayPredicate = SboostPredicate<ByteArrayType>;

        template<typename DTYPE>
        class DictEq : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
        private:
            const T &target_;
            int rawTarget_;
        public:
            DictEq(const T &target);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictEq<DTYPE>> build(const T &target);
        };

        using Int32DictEq = DictEq<Int32Type>;
        using DoubleDictEq = DictEq<DoubleType>;
        using ByteArrayDictEq = DictEq<ByteArrayType>;

        template<typename DTYPE>
        class DictLess : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &target_;
            int rawTarget_;
        public:
            DictLess(const T &target);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictLess<DTYPE>> build(const T &target);
        };

        using Int32DictLess = DictLess<Int32Type>;
        using DoubleDictLess = DictLess<DoubleType>;
        using ByteArrayDictLess = DictLess<ByteArrayType>;

        template<typename DTYPE>
        class DictBetween : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &lower_;
            const T &upper_;
            int rawLower_;
            int rawUpper_;
        public:
            DictBetween(const T &lower, const T &upper);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictBetween<DTYPE>> build(const T &lower, const T &upper);
        };

        using Int32DictBetween = DictBetween<Int32Type>;
        using DoubleDictBetween = DictBetween<DoubleType>;
        using ByteArrayDictBetween = DictBetween<ByteArrayType>;

        template<typename DTYPE>
        class DictMultiEq : public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            function<bool(const T &)> predicate_;
            unique_ptr<vector<uint32_t>> keys_;
        public:
            DictMultiEq(function<bool(const T &)> pred);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DictMultiEq<DTYPE>> build(function<bool(const T &)>);
        };

        using Int32DictMultiEq= DictMultiEq<Int32Type>;
        using DoubleDictMultiEq= DictMultiEq<DoubleType>;
        using ByteArrayDictMultiEq= DictMultiEq<ByteArrayType>;

        class DeltaEq : public RawAccessor<Int32Type> {
        private:
            const int target_;
        public:
            DeltaEq(const int target);

            void processDict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DeltaEq> build(const int target);
        };

        class DeltaLess : public RawAccessor<Int32Type> {
        private:
            const int target_;
        public:
            DeltaLess(const int target);

            void processDict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DeltaLess> build(const int target);
        };

        class DeltaBetween : public RawAccessor<Int32Type> {
        private:
            const int lower_;
            const int upper_;
        public:
            DeltaBetween(const int lower, const int upper);

            void processDict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;

            static unique_ptr<DeltaBetween> build(const int lower, const int upper);
        };
    }

    class ColFilter : public Filter {
    protected:
        vector<unique_ptr<ColPredicate>> predicates_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        ColFilter(initializer_list<ColPredicate *>);

        virtual ~ColFilter();

        shared_ptr<Table> filter(Table &input) override;
    };

    class RowFilter : public Filter {
    private:
        function<bool(DataRow &)> predicate_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        RowFilter(function<bool(DataRow &)> pred);

        virtual ~RowFilter() {}
    };
}
#endif //LQF_OPERATOR_FILTER_H
