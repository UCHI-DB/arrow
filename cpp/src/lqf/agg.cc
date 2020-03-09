//
// Created by harper on 2/21/20.
//

#include "agg.h"

using namespace std;
using namespace std::placeholders;
namespace lqf {

    AggReducer::AggReducer(uint32_t numHeaders, initializer_list<AggField *> fields) : header_(numHeaders),
                                                                                       fields_() {
        auto it = fields.begin();
        while (it != fields.end()) {
            fields_.push_back(unique_ptr<AggField>(*it));
            it++;
        }
    }

    MemDataRow &AggReducer::header() {
        return header_;
    }

    void AggReducer::reduce(DataRow &row) {
        for (uint32_t i = 0; i < fields_.size(); ++i) {
            fields_[i]->reduce(row);
        }
    }

    void AggReducer::merge(AggReducer &reducer) {
        for (uint32_t i = 0; i < fields_.size(); ++i) {
            this->fields_[i]->merge(*reducer.fields_[i]);
        }
    }

    void AggReducer::dump(DataRow &target) {
        for (uint32_t i = 0; i < header_.size(); ++i) {
            target[i] = header_[i];
        }
        for (uint32_t i = 0; i < fields_.size(); ++i) {
            fields_[i]->dump(target, i + header_.size());
        }
    }

    AggField::AggField(uint32_t index) : readIndex_(index) {}

    namespace agg {
        template<typename T, typename ACC>
        Sum<T, ACC>::Sum(uint32_t rIndex) : AggField(rIndex), value_(0) {}

        template<typename T, typename ACC>
        void Sum<T, ACC>::merge(AggField &another) {
            value_ += static_cast<Sum<T, ACC> &>(another).value_;
        }

        template<typename T, typename ACC>
        void Sum<T, ACC>::reduce(DataRow &input) {
            value_ += ACC::get(input[readIndex_]);
        }

        template<typename T, typename ACC>
        void Sum<T, ACC>::dump(DataRow &output, uint32_t index) {
            output[index] = value_;
        }

        template<typename T, typename ACC>
        Max<T, ACC>::Max(uint32_t rIndex) : AggField(rIndex), value_(0) {}

        template<typename T, typename ACC>
        void Max<T, ACC>::merge(AggField &another) {
            value_ = std::max(value_, static_cast<Max<T, ACC> &>(another).value_);
        }

        template<typename T, typename ACC>
        void Max<T, ACC>::reduce(DataRow &input) {
            value_ = std::max(value_, ACC::get(input[readIndex_]));
        }

        template<typename T, typename ACC>
        void Max<T, ACC>::dump(DataRow &output, uint32_t index) {
            output[index] = value_;
        }

        template<typename T, typename ACC>
        Avg<T, ACC>::Avg(uint32_t rIndex) : AggField(rIndex), value_(0), count_(0) {}

        template<typename T, typename ACC>
        void Avg<T, ACC>::merge(AggField &another) {
            value_ += static_cast<Avg<T, ACC> &>(another).value_;
            count_ += static_cast<Avg<T, ACC> &>(another).count_;
        }

        template<typename T, typename ACC>
        void Avg<T, ACC>::reduce(DataRow &input) {
            value_ += ACC::get(input[readIndex_]);
            count_ += 1;
        }

        template<typename T, typename ACC>
        void Avg<T, ACC>::dump(DataRow &output, uint32_t index) {
            output[index] = static_cast<double>(value_) / count_;
        }

        Count::Count() : AggField(-1), count_(0) {}

        void Count::merge(AggField &another) {
            count_ += static_cast<Count &>(another).count_;
        }

        void Count::reduce(DataRow &input) {
            count_ += 1;
        }

        void Count::dump(DataRow &output, uint32_t index) {
            output[index] = count_;
        }

        template class Sum<int32_t, AsInt>;
        template class Sum<double, AsDouble>;
        template class Max<int32_t, AsInt>;
        template class Max<double, AsDouble>;
        template class Avg<int32_t, AsInt>;
        template class Avg<double, AsDouble>;
    }

    HashCore::HashCore(uint32_t numFields, function<uint64_t(DataRow &)> hasher,
                       function<unique_ptr<AggReducer>(DataRow &)> headerInit)
            : numFields_(numFields), container_(), hasher_(hasher), headerInit_(headerInit) {}

    HashCore::~HashCore() {
        container_.clear();
    }

    uint32_t HashCore::size() {
        return container_.size();
    }

    uint32_t HashCore::numFields() {
        return numFields_;
    }

    void HashCore::consume(DataRow &row) {
        uint64_t key = hasher_(row);
        auto exist = container_.find(key);
        if (exist != container_.end()) {
            exist->second->reduce(row);
        } else {
            auto newReducer = headerInit_(row);
            newReducer->reduce(row);
            container_[key] = move(newReducer);
        }
    }

    void HashCore::dump(MemBlock &block) {
        auto it = container_.begin();
        auto wit = block.rows();
        for (uint32_t i = 0; i < container_.size(); ++i) {
            it->second->dump((*wit)[i]);
            it++;
        }
    }

    void HashCore::reduce(HashCore &another) {
        auto it = another.container_.begin();
        while (it != another.container_.end()) {
            auto exist = container_.find(it->first);
            if (exist == container_.end()) {
                container_[it->first] = move(it->second);
            } else {
                exist->second->merge(*it->second);
            }
            it++;
        }
    }

    TableCore::TableCore(uint32_t numFields, uint32_t tableSize, function<uint32_t(DataRow &)> indexer,
                         function<unique_ptr<AggReducer>(DataRow &)> headerInit)
            : numFields_(numFields), container_(tableSize), indexer_(indexer), headerInit_(headerInit) {}

    uint32_t TableCore::size() {
        return container_.size();
    }

    TableCore::~TableCore() {
        container_.clear();
    }

    uint32_t TableCore::numFields() {
        return numFields_;
    }

    void TableCore::consume(DataRow &row) {
        uint32_t index = indexer_(row);
        if (__builtin_expect(container_[index] == nullptr, 0)) {
            container_[index] = headerInit_(row);
        }
        container_[index]->reduce(row);
    }

    void TableCore::reduce(TableCore &another) {
        for (uint32_t i = 0; i < container_.size(); ++i) {
            container_[i]->merge(*(another.container_[i]));
        }
    }

    void TableCore::dump(MemBlock &block) {
        auto rows = block.rows();
        for (uint32_t i = 0; i < container_.size(); ++i) {
            container_[i]->dump((*rows)[i]);
        }
    }

    SimpleCore::SimpleCore(uint32_t numFields, function<unique_ptr<AggReducer>(DataRow &)> headerInit)
            : numFields_(numFields), headerInit_(headerInit) {}

    uint32_t SimpleCore::size() {
        return 1;
    }

    uint32_t SimpleCore::numFields() {
        return numFields_;
    }

    void SimpleCore::consume(DataRow &row) {
        if (__builtin_expect(reducer_ == nullptr, 0)) {
            reducer_ = headerInit_(row);
        }
        reducer_->reduce(row);
    }

    void SimpleCore::reduce(SimpleCore &another) {
        reducer_->merge(*(another.reducer_));
    }

    void SimpleCore::dump(MemBlock &block) {
        reducer_->dump((*block.rows())[0]);
    }
}
