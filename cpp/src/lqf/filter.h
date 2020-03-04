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
        function<bool(DataField & )> predicate_;
    public:
        SimpleColPredicate(uint32_t, function<bool(DataField & )>);

        ~SimpleColPredicate() {}

        shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) override;
    };

    namespace sboost {

        template<typename DTYPE>
        class DictEq : public ColPredicate, public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
        private:
            T target_;
            int rawTarget_;
        public:
            DictEq(uint32_t index, T target) : ColPredicate(index), target_(target) {}

            void processDict(Dictionary <DTYPE> &dict) override {
                rawTarget_ = dict.lookup(target_);
            }

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override {
                uint8_t bitWidth = data[0];
                ::sboost::encoding::rlehybrid::equal(data + 1, bitmap, bitmap_offset, bitWidth,
                                                     numEntry, rawTarget_);
            }
        };

        using Int32DictEq = DictEq<Int32Type>;
        using DoubleDictEq = DictEq<DoubleType>;

        template<typename DTYPE>
        class DictLess : public ColPredicate, public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            T target_;
            int rawTarget_;
        public:
            DictLess(uint32_t index, T target) : ColPredicate(index), target_(target) {}

            void processDict(Int32Dictionary &dict) override {
                rawTarget_ = dict.lookup(target_);
            };

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override {
                uint8_t bitWidth = data[0];
                ::sboost::encoding::rlehybrid::less(data + 1, bitmap, bitmap_offset, bitWidth,
                                                    numEntry, rawTarget_);
            }
        };

        using Int32DictLess = DictLess<Int32Type>;
        using DoubleDictLess = DictLess<DoubleType>;

        template<typename DTYPE>
        class DictBetween : public ColPredicate, public RawAccessor<DTYPE> {
            using T = typename DTYPE::c_type;
            T lower_;
            T upper_;
            int rawLower_;
            int rawUpper_;
        public:
            DictBetween(uint32_t index, T lower, T upper) : ColPredicate(index), lower_(lower), upper_(upper) {}

            void processDict(Int32Dictionary &dict) override {
                rawLower_ = dict.lookup(lower_);
                rawUpper_ = dict.lookup(upper_);
            };

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override {
                uint8_t bitWidth = data[0];
                ::sboost::encoding::rlehybrid::between(data + 1, bitmap, bitmap_offset, bitWidth,
                                                       numEntry, rawLower_, rawUpper_);
            }
        };

        using Int32DictBetween = DictBetween<Int32Type>;
        using DoubleDictBetween = DictBetween<DoubleType>;

        class DeltaEq : public ColPredicate, public Int32Accessor {
        private:
            int target_;
        public:
            DeltaEq(uint32_t index, int target) : ColPredicate(index), target_(target) {}

            void processDict(Int32Dictionary &) override {}

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override {
                ::sboost::encoding::deltabp::equal(data, bitmap, bitmap_offset, numEntry, target_);
            }
        };

        class DeltaLess : public ColPredicate, public Int32Accessor {
        private:
            int target_;
        public:
            DeltaLess(uint32_t index, int target) : ColPredicate(index), target_(target) {}

            void processDict(Int32Dictionary &) override {}

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override {
                ::sboost::encoding::deltabp::less(data, bitmap, bitmap_offset, numEntry, target_);
            }
        };

        class DeltaBetween : public ColPredicate, public Int32Accessor {
        private:
            int lower_;
            int upper_;
        public:
            DeltaBetween(uint32_t index, int lower, int upper)
                    : ColPredicate(index), lower_(lower), upper_(upper) {}

            void processDict(Int32Dictionary &) override {}

            void scanPage(uint64_t numEntry, const uint8_t *data,
                          uint64_t *bitmap, uint64_t bitmap_offset) override {
                ::sboost::encoding::deltabp::between(data, bitmap, bitmap_offset, numEntry, lower_, upper_);
            }
        };
    }

    class ColFilter : public Filter {
    protected:
        vector <unique_ptr<ColPredicate>> predicates_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        ColFilter(initializer_list<ColPredicate *>);

        virtual ~ColFilter();
    };

    class RowFilter : public Filter {
    private:
        function<bool(DataRow & )> predicate_;

        virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

    public:
        RowFilter(function<bool(DataRow & )> pred);

        virtual ~RowFilter() {}
    };
}
#endif //LQF_OPERATOR_FILTER_H
