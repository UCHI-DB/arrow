//
// Created by harper on 3/22/20.
//

#ifndef ARROW_FILTER_EXECUTOR_H
#define ARROW_FILTER_EXECUTOR_H

#include <functional>
#include <memory>
#include <cstring>
#include <unordered_map>
#include <vector>
#include "bitmap.h"
#include "data_model.h"
#include "filter.h"

namespace lqf {

    class FilterExecutor {
    protected:
        unordered_map<string, unique_ptr<vector<ColPredicate *>>> regTable_;
        unordered_map<string, unique_ptr<unordered_map<ColPredicate *, shared_ptr<Bitmap>>>> result_;

        string makeKey(Table &, uint32_t);

    public:

        static unique_ptr<FilterExecutor> inst;

        FilterExecutor();

        void reg(Table &, ColPredicate &);

        shared_ptr<Bitmap> executeSimple(Block &, Bitmap &, SimpleColPredicate &);

        template<typename DTYPE>
        shared_ptr<Bitmap> executeSboost(Block &, sboost::SboostPredicate<DTYPE> &);
    };

    template<typename DTYPE>
    class PackedRawAccessor : public RawAccessor<DTYPE> {
    private:
        vector<unique_ptr<RawAccessor<DTYPE>>> &content_;
    public:
        PackedRawAccessor(vector<unique_ptr<RawAccessor<DTYPE>>> &content);

        void init(uint64_t size) override;

        void dict(Dictionary<DTYPE> &) override;

        void data(DataPage *dpage) override;
    };
}
#endif //ARROW_FILTER_EXECUTOR_H
