//
// Created by harper on 2/21/20.
//

#include "filter.h"
#include <sboost/encoding/rlehybrid.h>
#include <functional>

using namespace std;
using namespace std::placeholders;
using namespace sboost::encoding;
using namespace lqf;

namespace lqf {

    shared_ptr<Table> Filter::filter(Table &input) {
        function<shared_ptr<Block>(
                const shared_ptr<Block> &)> mapper = bind(&Filter::processBlock, this, _1);
        return make_shared<TableView>(input.numFields(), input.blocks()->map(mapper));
    }

    shared_ptr<Block> Filter::processBlock(const shared_ptr<Block> &input) {
        shared_ptr<Bitmap> result = filterBlock(*input);
        return input->mask(result);
    }

    ColPredicate::ColPredicate(uint32_t index) : index_(index) {}

    ColPredicate::~ColPredicate() {}

    SimpleColPredicate::SimpleColPredicate(uint32_t index, function<bool(DataField &)> pred)
            : ColPredicate(index), predicate_(pred) {}

    shared_ptr<Bitmap> SimpleColPredicate::filterBlock(Block &block, Bitmap &skip) {
        auto result = make_shared<SimpleBitmap>(block.size());
        uint64_t block_size = block.size();
        auto ite = block.col(index_);
        if (skip.isFull()) {
            for (uint64_t i = 0; i < block_size; ++i) {
                if (predicate_((*ite)[i])) {
                    result->put(i);
                }
            }
        } else {
            auto posite = skip.iterator();
            while (posite->hasNext()) {
                auto pos = posite->next();
                if (predicate_((*ite)[pos])) {
                    result->put(pos);
                }
            }
        }
        return result;
    }

    ColFilter::ColFilter(initializer_list<ColPredicate *> preds) {
        predicates_ = vector<unique_ptr<ColPredicate>>();
        auto it = preds.begin();
        while (it < preds.end()) {
            predicates_.push_back(unique_ptr<ColPredicate>(*it));
            it++;
        }
    }

    ColFilter::~ColFilter() { predicates_.clear(); }

    shared_ptr<Bitmap> ColFilter::filterBlock(Block &input) {
        shared_ptr<Bitmap> result = make_shared<FullBitmap>(input.size());
        auto it = predicates_.begin();
        while (it != predicates_.end()) {
            result = (*it)->filterBlock(input, *result);
            it++;
        }
        return result;
    }

    RowFilter::RowFilter(function<bool(DataRow &)> pred) : predicate_(pred) {}

    shared_ptr<Bitmap> RowFilter::filterBlock(Block &input) {
        auto result = make_shared<SimpleBitmap>(input.size());
        auto rit = input.rows();

        for (uint32_t i = 0; i < input.size(); ++i) {
            if (predicate_((*rit).next())) {
                result->put(i);
            }
        }
        return result;
    }

    namespace sboost {

        template<typename DTYPE>
        shared_ptr<Bitmap> SboostPredicate<DTYPE>::filterBlock(Block &block, Bitmap &) {
            return static_cast<ParquetBlock &>(block).raw(index_, this);
        }

        template<typename DTYPE>
        DictEq<DTYPE>::DictEq(uint32_t index, const T &target) : SboostPredicate<DTYPE>(index), target_(target) {}

        template<typename DTYPE>
        void DictEq<DTYPE>::processDict(Dictionary<DTYPE> &dict) {
            rawTarget_ = dict.lookup(target_);
        }

        template<typename DTYPE>
        void DictEq<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                     uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::equal(data + 1, bitmap, bitmap_offset, bitWidth,
                                                 numEntry, rawTarget_);
        }

        template<typename DTYPE>
        DictLess<DTYPE>::DictLess(uint32_t index, const T &target) : SboostPredicate<DTYPE>(index), target_(target) {}

        template<typename DTYPE>
        void DictLess<DTYPE>::processDict(Dictionary<DTYPE> &dict) {
            rawTarget_ = dict.lookup(target_);
        };

        template<typename DTYPE>
        void DictLess<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                       uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::less(data + 1, bitmap, bitmap_offset, bitWidth,
                                                numEntry, rawTarget_);
        }

        template<typename DTYPE>
        DictBetween<DTYPE>::DictBetween(uint32_t index, const T &lower, const T &upper)
                : SboostPredicate<DTYPE>(index), lower_(lower), upper_(upper) {}

        template<typename DTYPE>
        void DictBetween<DTYPE>::processDict(Dictionary<DTYPE> &dict) {
            rawLower_ = dict.lookup(lower_);
            rawUpper_ = dict.lookup(upper_);
        };

        template<typename DTYPE>
        void DictBetween<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::between(data + 1, bitmap, bitmap_offset, bitWidth,
                                                   numEntry, rawLower_, rawUpper_);
        }

        DeltaEq::DeltaEq(uint32_t index, const int target) : SboostPredicate<Int32Type>(index), target_(target) {}

        void DeltaEq::processDict(Int32Dictionary &) {}

        void DeltaEq::scanPage(uint64_t numEntry, const uint8_t *data,
                               uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::equal(data, bitmap, bitmap_offset, numEntry, target_);
        }

        DeltaLess::DeltaLess(uint32_t index, const int target) : SboostPredicate(index), target_(target) {}

        void DeltaLess::processDict(Int32Dictionary &) {}

        void DeltaLess::scanPage(uint64_t numEntry, const uint8_t *data,
                                 uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::less(data, bitmap, bitmap_offset, numEntry, target_);
        }

        DeltaBetween::DeltaBetween(uint32_t index, const int lower, const int upper)
                : SboostPredicate(index), lower_(lower), upper_(upper) {}

        void DeltaBetween::processDict(Int32Dictionary &) {}

        void DeltaBetween::scanPage(uint64_t numEntry, const uint8_t *data,
                                    uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::between(data, bitmap, bitmap_offset, numEntry, lower_, upper_);
        }

        template class DictEq<Int32Type>;
        template class DictEq<DoubleType>;
        template class DictEq<ByteArrayType>;
        template class DictLess<Int32Type>;
        template class DictLess<DoubleType>;
        template class DictLess<ByteArrayType>;
        template class DictBetween<Int32Type>;
        template class DictBetween<DoubleType>;
        template class DictBetween<ByteArrayType>;
    }
}


