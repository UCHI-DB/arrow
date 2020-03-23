//
// Created by harper on 3/22/20.
//

#include "filter_executor.h"

namespace lqf {

    using namespace sboost;

    unique_ptr<FilterExecutor> FilterExecutor::inst = unique_ptr<FilterExecutor>(new FilterExecutor());

    FilterExecutor::FilterExecutor() {}

    void FilterExecutor::reg(Table &table, ColPredicate &predicate) {
        if (predicate.supportBatch()) {
            auto key = makeKey(table, predicate.index());
            auto place = regTable_.emplace(key, new vector<ColPredicate *>());
            (*place.first).second->push_back(&predicate);
        }
    }

    shared_ptr<Bitmap> FilterExecutor::executeSimple(Block &block, Bitmap &skip, SimpleColPredicate &predicate) {
        return predicate.filterBlock(block, skip);
    }

    using namespace lqf::sboost;

    template<typename DTYPE>
    shared_ptr<Bitmap> FilterExecutor::executeSboost(Block &block, SboostPredicate<DTYPE> &predicate) {
        auto key = makeKey(*block.owner(), predicate.index());
        auto found = result_.find(key);
        if (found != result_.end()) {
            return (*(found->second))[&predicate];
        }
        auto ite = regTable_.find(key);
        if (ite != regTable_.end()) {
            auto preds = *(ite->second).get();
            vector<unique_ptr<RawAccessor<DTYPE>>> content;

            for (auto pred: preds) {
                auto spred = dynamic_cast<SboostPredicate<DTYPE> *>(pred);
                content.push_back(spred->build());
            }

            PackedRawAccessor<DTYPE> packedAccessor(content);

            ParquetBlock &pblock = dynamic_cast<ParquetBlock &>(block);
            pblock.raw(predicate.index(), &packedAccessor);

            auto resultMap = new unordered_map<ColPredicate *, shared_ptr<Bitmap>>();
            for (uint32_t i = 0; i < preds.size(); ++i) {
                (*resultMap)[preds[i]] = content[i]->result();
            }
            result_[key] = unique_ptr<unordered_map<ColPredicate *, shared_ptr<Bitmap>>>(resultMap);
            return (*resultMap)[&predicate];
        }
        return nullptr;
    }

    string FilterExecutor::makeKey(Table &table, uint32_t index) {
        stringstream ss;
        ss << (uint64_t) &table << "." << index;
        return ss.str();
    }

    template<typename DTYPE>
    PackedRawAccessor<DTYPE>::PackedRawAccessor(vector<unique_ptr<RawAccessor<DTYPE>>> &content)
            :content_(content) {}

    template<typename DTYPE>
    void PackedRawAccessor<DTYPE>::init(uint64_t size) {
        for (auto const &item: content_) {
            item->init(size);
        }
    }

    template<typename DTYPE>
    void PackedRawAccessor<DTYPE>::dict(Dictionary<DTYPE> &dict) {
        for (auto const &item: content_) {
            item->dict(dict);
        }
    }

    template<typename DTYPE>
    void PackedRawAccessor<DTYPE>::data(DataPage *dpage) {
        for (auto const &item: content_) {
            item->data(dpage);
        }
    }

    template shared_ptr<Bitmap>
    FilterExecutor::executeSboost<Int32Type>(Block &block, SboostPredicate<Int32Type> &predicate);

    template shared_ptr<Bitmap>
    FilterExecutor::executeSboost<ByteArrayType>(Block &block, SboostPredicate<ByteArrayType> &predicate);

    template shared_ptr<Bitmap>
    FilterExecutor::executeSboost<DoubleType>(Block &block, SboostPredicate<DoubleType> &predicate);
}