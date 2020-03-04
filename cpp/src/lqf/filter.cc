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
        return make_shared<TableView>(input.blocks()->map(mapper));
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
}
