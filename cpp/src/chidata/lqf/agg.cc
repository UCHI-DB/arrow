//
// Created by harper on 2/21/20.
//

#include "agg.h"

using namespace std;
using namespace std::placeholders;
namespace chidata {
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

        DoubleSum::DoubleSum(uint32_t rIndex) : AggField(rIndex), value_(0) {}

        void DoubleSum::merge(AggField &another) {
            value_ += static_cast<DoubleSum &>(another).value_;
        }

        void DoubleSum::reduce(DataRow &input) {
            value_ += input[readIndex_].asDouble();
        }

        void DoubleSum::dump(DataRow &output, uint32_t index) {
            output[index] = value_;
        }

        DoubleMax::DoubleMax(uint32_t rIndex) : AggField(rIndex), value_(0) {}

        void DoubleMax::merge(AggField &another) {
            value_ = max(value_, static_cast<DoubleMax &>(another).value_);
        }

        void DoubleMax::reduce(DataRow &input) {
            value_ = max(value_, input[readIndex_].asDouble());
        }

        void DoubleMax::dump(DataRow &output, uint32_t index) {
            output[index] = value_;
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
}