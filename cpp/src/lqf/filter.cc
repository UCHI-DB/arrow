//
// Created by harper on 2/21/20.
//

#include "filter.h"
#include "filter_executor.h"
#include <sboost/encoding/rlehybrid.h>
#include <functional>
#include <sboost/simd.h>
#include <sboost/bitmap_writer.h>

using namespace std;
using namespace std::placeholders;
using namespace sboost::encoding;

namespace lqf {

    shared_ptr<Table> Filter::filter(Table &input) {
        function<shared_ptr<Block>(
                const shared_ptr<Block> &)> mapper = bind(&Filter::processBlock, this, _1);
        return make_shared<TableView>(input.colSize(), input.blocks()->map(mapper));
    }

    shared_ptr<Block> Filter::processBlock(const shared_ptr<Block> &input) {
        shared_ptr<Bitmap> result = filterBlock(*input);
        return input->mask(result);
    }

    ColPredicate::ColPredicate(uint32_t index) : index_(index) {}

    ColPredicate::~ColPredicate() {}

    SimplePredicate::SimplePredicate(uint32_t index, function<bool(const DataField &)> pred)
            : ColPredicate(index), predicate_(pred) {}

    shared_ptr<Bitmap> SimplePredicate::filterBlock(Block &block, Bitmap &skip) {
        auto result = make_shared<SimpleBitmap>(block.limit());

        auto ite = block.col(index_);
        if (skip.isFull()) {
            auto block_size = block.size();
            for (uint64_t i = 0; i < block_size; ++i) {
                if (predicate_(ite->next())) {
                    result->put(ite->pos());
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

    ColFilter::ColFilter(ColPredicate *pred) {
        predicates_.push_back(unique_ptr<ColPredicate>(pred));
    }

    ColFilter::ColFilter(initializer_list<ColPredicate *> preds) {
        for (auto &pred: preds) {
            predicates_.push_back(unique_ptr<ColPredicate>(pred));
        }
    }

    ColFilter::~ColFilter() { predicates_.clear(); }

    shared_ptr<Table> ColFilter::filter(Table &input) {
        for (auto &pred: predicates_) {
            FilterExecutor::inst->reg(input, *pred);
        }
        return Filter::filter(input);
    }

    shared_ptr<Bitmap> ColFilter::filterBlock(Block &input) {
        shared_ptr<Bitmap> result = make_shared<FullBitmap>(input.limit());
        for (auto &pred: predicates_) {
            result = pred->filterBlock(input, *result);
        }
        return result;
    }

    RowFilter::RowFilter(function<bool(DataRow &)> pred) : predicate_(pred) {}

    shared_ptr<Bitmap> RowFilter::filterBlock(Block &input) {
        auto result = make_shared<SimpleBitmap>(input.limit());
        auto rit = input.rows();

        auto block_size = input.size();
        for (uint32_t i = 0; i < block_size; ++i) {
            if (predicate_((*rit).next())) {
                result->put(i);
            }
        }
        return result;
    }

    KeyFinder::KeyFinder(uint32_t key_index, function<bool(DataRow &)> pred)
            : key_index_(key_index), predicate_(pred) {}

    int32_t KeyFinder::find(Table &table) {
        function<int32_t(const shared_ptr<Block> &)> mapper = bind(&KeyFinder::filterBlock, this,
                                                                   placeholders::_1);
        auto values = table.blocks()->map(mapper)->collect();
        return (*values)[0];
    }

    int32_t KeyFinder::filterBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto block_size = block->size();
        for (uint32_t i = 0; i < block_size; ++i) {
            DataRow &row = rows->next();
            if (predicate_(row)) {
                return row[key_index_].asInt();
            }
        }
        return -1;
    }

    MapFilter::MapFilter(uint32_t key_index, unordered_set<uint64_t> &filter)
            : key_index_(key_index), filter_(filter) {}

    shared_ptr<Bitmap> MapFilter::filterBlock(Block &input) {
        auto col = input.col(key_index_);
        auto bitmap = make_shared<SimpleBitmap>(input.limit());
        auto block_size = input.size();
        for (uint32_t i = 0; i < block_size; ++i) {
            if (filter_.find(col->next().asInt()) != filter_.end()) {
                bitmap->put(i);
            }
        }
        return bitmap;
    }

    PowerMapFilter::PowerMapFilter(function<uint64_t(DataRow &)> key_maker, unordered_set<uint64_t> &filter)
            : key_maker_(key_maker), filter_(filter) {}

    shared_ptr<Bitmap> PowerMapFilter::filterBlock(Block &input) {
        auto rows = input.rows();
        auto bitmap = make_shared<SimpleBitmap>(input.limit());
        auto block_size = input.size();
        for (uint32_t i = 0; i < block_size; ++i) {
            if (filter_.find(key_maker_(rows->next())) != filter_.end()) {
                bitmap->put(i);
            }
        }
        return bitmap;
    }

    namespace raw {
        template<typename DTYPE>
        Not<DTYPE>::Not(unique_ptr<lqf::RawAccessor<DTYPE>> inner):inner_(move(inner)) {}

        template<typename DTYPE>
        void Not<DTYPE>::init(uint64_t size) {
            inner_->init(size);
        }

        template<typename DTYPE>
        void Not<DTYPE>::dict(lqf::Dictionary<DTYPE> &dict) {
            inner_->dict(dict);
        }

        template<typename DTYPE>
        void Not<DTYPE>::data(DataPage *dpage) {
            inner_->data(dpage);
        }

        template<typename DTYPE>
        shared_ptr<Bitmap> Not<DTYPE>::result() {
            return ~(*inner_->result());
        }

        template<typename DTYPE>
        unique_ptr<RawAccessor<DTYPE>> Not<DTYPE>::build(function<unique_ptr<RawAccessor<DTYPE>>()> builder) {
            return unique_ptr<RawAccessor<DTYPE>>(new Not<DTYPE>(builder()));
        }

        template
        class Not<Int32Type>;

        template
        class Not<DoubleType>;

        template
        class Not<ByteArrayType>;
    }

    namespace sboost {

        template<typename DTYPE>
        shared_ptr<Bitmap> SboostPredicate<DTYPE>::filterBlock(Block &block, Bitmap &skip) {
//            unique_ptr<RawAccessor<DTYPE>> accessor = builder_();
//            return dynamic_cast<ParquetBlock &>(block).raw(index_, accessor.get());
            return skip & *FilterExecutor::inst->executeSboost(block, *this);
        }

        template<typename DTYPE>
        unique_ptr<RawAccessor<DTYPE>> SboostPredicate<DTYPE>::build() {
            return builder_();
        }

        template<typename DTYPE>
        DictEq<DTYPE>::DictEq(const T &target) : target_(target) {}

        template<typename DTYPE>
        void DictEq<DTYPE>::dict(Dictionary<DTYPE> &dict) {
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
        unique_ptr<DictEq<DTYPE>> DictEq<DTYPE>::build(const T &target) {
            return unique_ptr<DictEq<DTYPE>>(new DictEq<DTYPE>(target));
        }

        template<typename DTYPE>
        DictLess<DTYPE>::DictLess(const T &target) : target_(target) {}

        template<typename DTYPE>
        void DictLess<DTYPE>::dict(Dictionary<DTYPE> &dict) {
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
        unique_ptr<DictLess<DTYPE>> DictLess<DTYPE>::build(const T &target) {
            return unique_ptr<DictLess<DTYPE>>(new DictLess<DTYPE>(target));
        }

        template<typename DTYPE>
        DictGreater<DTYPE>::DictGreater(const T &target) : target_(target) {}

        template<typename DTYPE>
        void DictGreater<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawTarget_ = dict.lookup(target_);
        };

        template<typename DTYPE>
        void DictGreater<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::greater(data + 1, bitmap, bitmap_offset, bitWidth,
                                                   numEntry, rawTarget_);
        }

        template<typename DTYPE>
        unique_ptr<DictGreater<DTYPE>> DictGreater<DTYPE>::build(const T &target) {
            return unique_ptr<DictGreater<DTYPE>>(new DictGreater<DTYPE>(target));
        }


        template<typename DTYPE>
        DictBetween<DTYPE>::DictBetween(const T &lower, const T &upper)
                : lower_(lower), upper_(upper) {}

        template<typename DTYPE>
        void DictBetween<DTYPE>::dict(Dictionary<DTYPE> &dict) {
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

        template<typename DTYPE>
        unique_ptr<DictBetween<DTYPE>> DictBetween<DTYPE>::build(const T &lower, const T &upper) {
            return unique_ptr<DictBetween<DTYPE>>(new DictBetween<DTYPE>(lower, upper));
        }

        template<typename DTYPE>
        DictRangele<DTYPE>::DictRangele(const T &lower, const T &upper)
                : lower_(lower), upper_(upper) {}

        template<typename DTYPE>
        void DictRangele<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            rawLower_ = dict.lookup(lower_);
            rawUpper_ = dict.lookup(upper_);
        };

        template<typename DTYPE>
        void DictRangele<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];
            ::sboost::encoding::rlehybrid::rangele(data + 1, bitmap, bitmap_offset, bitWidth,
                                                   numEntry, rawLower_, rawUpper_);
        }

        template<typename DTYPE>
        unique_ptr<DictRangele<DTYPE>> DictRangele<DTYPE>::build(const T &lower, const T &upper) {
            return unique_ptr<DictRangele<DTYPE>>(new DictRangele<DTYPE>(lower, upper));
        }


        template<typename DTYPE>
        DictMultiEq<DTYPE>::DictMultiEq(function<bool(const T &)> pred) : predicate_(pred) {}

        template<typename DTYPE>
        void DictMultiEq<DTYPE>::dict(Dictionary<DTYPE> &dict) {
            keys_ = move(dict.list(predicate_));
        };

        template<typename DTYPE>
        void DictMultiEq<DTYPE>::scanPage(uint64_t numEntry, const uint8_t *data,
                                          uint64_t *bitmap, uint64_t bitmap_offset) {
            uint8_t bitWidth = data[0];

            uint32_t buffer_size = (numEntry + 63) >> 6;
            uint64_t *page_oneresult = (uint64_t *) aligned_alloc(64, sizeof(uint64_t) * buffer_size);
            uint64_t *page_result = (uint64_t *) aligned_alloc(64, sizeof(uint64_t) * buffer_size);

            memset((void *) page_result, 0, sizeof(uint64_t) * buffer_size);

            auto ite = keys_.get()->begin();
            ::sboost::encoding::rlehybrid::equal(data + 1, page_result, 0, bitWidth,
                                                 numEntry, *ite);
            ite++;
            while (ite != keys_.get()->end()) {
                memset((void *) page_oneresult, 0, sizeof(uint64_t) * buffer_size);
                ::sboost::encoding::rlehybrid::equal(data + 1, page_oneresult, 0, bitWidth,
                                                     numEntry, *ite);
                ::sboost::simd::simd_or(page_result, page_oneresult, buffer_size);
                ite++;
            }
            ::sboost::BitmapWriter writer(bitmap, bitmap_offset);
            writer.appendWord(page_result, numEntry);
        }

        template<typename DTYPE>
        unique_ptr<DictMultiEq<DTYPE>> DictMultiEq<DTYPE>::build(function<bool(const T &)> pred) {
            return unique_ptr<DictMultiEq<DTYPE>>(new DictMultiEq<DTYPE>(pred));
        }

        DeltaEq::DeltaEq(const int target) : target_(target) {}

        void DeltaEq::dict(Int32Dictionary &) {}

        void DeltaEq::scanPage(uint64_t numEntry, const uint8_t *data,
                               uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::equal(data, bitmap, bitmap_offset, numEntry, target_);
        }

        unique_ptr<DeltaEq> DeltaEq::build(const int target) {
            return unique_ptr<DeltaEq>(new DeltaEq(target));
        }

        DeltaLess::DeltaLess(const int target) : target_(target) {}

        void DeltaLess::dict(Int32Dictionary &) {}

        void DeltaLess::scanPage(uint64_t numEntry, const uint8_t *data,
                                 uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::less(data, bitmap, bitmap_offset, numEntry, target_);
        }

        unique_ptr<DeltaLess> DeltaLess::build(const int target) {
            return unique_ptr<DeltaLess>(new DeltaLess(target));
        }

        DeltaBetween::DeltaBetween(const int lower, const int upper)
                : lower_(lower), upper_(upper) {}

        void DeltaBetween::dict(Int32Dictionary &) {}

        void DeltaBetween::scanPage(uint64_t numEntry, const uint8_t *data,
                                    uint64_t *bitmap, uint64_t bitmap_offset) {
            ::sboost::encoding::deltabp::between(data, bitmap, bitmap_offset, numEntry, lower_, upper_);
        }

        unique_ptr<DeltaBetween> DeltaBetween::build(const int lower, const int upper) {
            return unique_ptr<DeltaBetween>(new DeltaBetween(lower, upper));
        }

        template
        class DictEq<Int32Type>;

        template
        class DictEq<DoubleType>;

        template
        class DictEq<ByteArrayType>;

        template
        class DictLess<Int32Type>;

        template
        class DictLess<DoubleType>;

        template
        class DictLess<ByteArrayType>;

        template
        class DictGreater<Int32Type>;

        template
        class DictGreater<DoubleType>;

        template
        class DictGreater<ByteArrayType>;

        template
        class DictBetween<Int32Type>;

        template
        class DictBetween<DoubleType>;

        template
        class DictBetween<ByteArrayType>;

        template
        class DictRangele<Int32Type>;

        template
        class DictRangele<DoubleType>;

        template
        class DictRangele<ByteArrayType>;

        template
        class DictMultiEq<Int32Type>;

        template
        class DictMultiEq<DoubleType>;

        template
        class DictMultiEq<ByteArrayType>;

        template
        class SboostPredicate<Int32Type>;

        template
        class SboostPredicate<DoubleType>;

        template
        class SboostPredicate<ByteArrayType>;
    }
}


