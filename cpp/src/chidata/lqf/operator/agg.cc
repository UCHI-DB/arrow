//
// Created by harper on 2/21/20.
//

#include "agg.h"

using namespace std;
using namespace std::placeholders;
namespace chidata {
    namespace lqf {
        namespace opr {

            AggReducer::AggReducer(initializer_list<unique_ptr<AggField>> fields) {
                fields_ = vector<unique_ptr<AggField>>();
                auto it = fields.begin();
                while (it != fields.end()) {
                    fields_.push_back(move(*it));
                    it++;
                }
            }

            void AggReducer::reduce(DataRow &row) {
                for (auto &field: fields_) {
                    field->reduce(row);
                }
            }

            void AggReducer::merge(AggReducer &reducer) {
                for (int i = 0; i < fields_.size(); ++i) {
                    this->fields_[i]->merge(*reducer.fields_[i]);
                }
            }

            void AggReducer::dump(DataRow &target) {
                for (auto &field: fields_) {
                    field->dump(target);
                }
            }

            namespace aggfield {

                DoubleSum::DoubleSum(uint32_t rIndex, uint32_t wIndex) : AggField(rIndex, wIndex), value_(0) {}

                void DoubleSum::merge(AggField &another) {
                    value_ += static_cast<DoubleSum &>(another).value_;
                }

                void DoubleSum::reduce(DataRow &input) {
                    value_ += input[readIndex_].asDouble();
                }

                void DoubleSum::dump(DataRow &output) {
                    output[writeIndex_] = value_;
                }

                Count::Count(uint32_t rIndex, uint32_t wIndex) : AggField(rIndex, wIndex), count_(0) {}

                void Count::merge(AggField &another) {
                    count_ += static_cast<Count &>(another).count_;
                }

                void Count::reduce(DataRow &input) {
                    count_ += 1;
                }

                void Count::dump(DataRow &output) {
                    output[writeIndex_] = count_;
                }
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
                for (int i = 0; i < container_.size(); ++i) {
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
}