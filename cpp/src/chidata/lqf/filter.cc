//
// Created by harper on 2/21/20.
//

#include "filter.h"

using namespace std;
using namespace std::placeholders;
namespace chidata {
    namespace lqf {

        shared_ptr<Table> Filter::filter(Table &input) {
            function<shared_ptr<Block>(const shared_ptr<Block> &)> mapper
                    = bind(&Filter::processBlock, this, _1);
            return make_shared<TableView>(input.blocks()->map(mapper));
        }

        shared_ptr<Block> Filter::processBlock(const shared_ptr<Block> &input) {
            shared_ptr<Bitmap> result = filterBlock(*input);
            return input->mask(result);
        }

        shared_ptr<Bitmap> SimpleColPredicate::filter(Block &block, Bitmap &skip) {
            auto result = make_shared<SimpleBitmap>(block.size());
            uint64_t block_size = block.size();
            auto ite = block.col(index_);
            if (skip.isFull()) {
                for (uint64_t i = 0; i < block_size; ++i) {
                    if (pred_((*ite)[i])) {
                        result->put(i);
                    }
                }
            } else {
                auto posite = skip.iterator();
                while (posite->hasNext()) {
                    auto pos = posite->next();
                    if (pred_((*ite)[pos])) {
                        result->put(pos);
                    }
                }
            }
            return result;
        }

        shared_ptr<Bitmap> SBoostPredicate::filter(Block &block, Bitmap &mask) {
            ParquetBlock &pblock = static_cast<ParquetBlock &>(block);

            throw invalid_argument("not implemented");
        }

        ColFilter::ColFilter() {}

        ColFilter::~ColFilter() { predicates_.clear(); }

        void ColFilter::install(unique_ptr<ColPredicate> pred) {
            predicates_.push_back(move(pred));
        }

        shared_ptr<Bitmap> ColFilter::filterBlock(Block &input) {
            shared_ptr<Bitmap> result = make_shared<SimpleBitmap>(input.size());
            auto it = predicates_.begin();
            while (it != predicates_.end()) {
                result = (*it)->filter(input, *result);
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
}