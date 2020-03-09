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

using namespace std;

namespace lqf {

    class Filter {
    protected:
        shared_ptr<Block> processBlock(const shared_ptr<Block> &);

        virtual shared_ptr<Bitmap> filterBlock(Block &) = 0;

    public:
        shared_ptr<Table> filter(Table &input);
    };

    class ColPredicate {
    protected:
        uint32_t index_;
    public:
        ColPredicate(uint32_t);

        virtual ~ColPredicate();

        virtual shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) = 0;
    };

    class SimpleColPredicate : public ColPredicate {
    private:
        function<bool(DataField &)> predicate_;
    public:
        SimpleColPredicate(uint32_t, function<bool(DataField &)>);

        ~SimpleColPredicate() {}

        shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) override;
    };

    namespace sboost {

        template<typename DTYPE>
        class SboostPredicate : public ColPredicate, public RawAccessor<DTYPE> {
        public:
            SboostPredicate(uint32_t index) : ColPredicate(index) {}

            shared_ptr<Bitmap> filterBlock(Block &block, Bitmap &) override;
        };

        template<typename DTYPE>
        class DictEq : public SboostPredicate<DTYPE> {
            using T = typename DTYPE::c_type;
        private:
            const T &target_;
            int rawTarget_;
        public:
            DictEq(uint32_t index, const T &target);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;
        };

        using Int32DictEq = DictEq<Int32Type>;
        using DoubleDictEq = DictEq<DoubleType>;
        using ByteArrayDictEq = DictEq<ByteArrayType>;

        template<typename DTYPE>
        class DictLess : public SboostPredicate<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &target_;
            int rawTarget_;
        public:
            DictLess(uint32_t index, const T &target);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;
        };

        using Int32DictLess = DictLess<Int32Type>;
        using DoubleDictLess = DictLess<DoubleType>;
        using ByteArrayDictLess = DictLess<ByteArrayType>;

        template<typename DTYPE>
        class DictBetween : public SboostPredicate<DTYPE> {
            using T = typename DTYPE::c_type;
            const T &lower_;
            const T &upper_;
            int rawLower_;
            int rawUpper_;
        public:
            DictBetween(uint32_t index, const T &lower, const T &upper);

            void processDict(Dictionary<DTYPE> &dict) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;
        };

        using Int32DictBetween = DictBetween<Int32Type>;
        using DoubleDictBetween = DictBetween<DoubleType>;
        using ByteArrayDictBetween = DictBetween<ByteArrayType>;

        class DeltaEq : public SboostPredicate<Int32Type> {
        private:
            const int target_;
        public:
            DeltaEq(uint32_t index, const int target);

            void processDict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;
        };

        class DeltaLess : public SboostPredicate<Int32Type> {
        private:
            const int target_;
        public:
            DeltaLess(uint32_t index, const int target);

            void processDict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;
        };

        class DeltaBetween : public SboostPredicate<Int32Type> {
        private:
            const int lower_;
            const int upper_;
        public:
            DeltaBetween(uint32_t index, const int lower, const int upper);

            void processDict(Int32Dictionary &) override;

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override;
        };
    }

    class ColFilter : public Filter {
    protected:
        vector<unique_ptr<ColPredicate>> predicates_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        ColFilter(initializer_list<ColPredicate *>);

        virtual ~ColFilter();
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
