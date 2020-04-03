//
// Created by harper on 2/21/20.
//

#include <climits>
#include "agg.h"

using namespace std;
using namespace std::placeholders;
namespace lqf {

    AggReducer::AggReducer(uint32_t numHeaders, initializer_list<AggField *> fields) :
            AggReducer(lqf::colSize(numHeaders), fields) {}


    AggReducer::AggReducer(const vector<uint32_t> &col_size, initializer_list<AggField *> fields)
            : header_(col_size) {
        for (auto &field:fields) {
            fields_.push_back(unique_ptr<AggField>(field));
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

    void AggReducer::dump(DataRowIterator &iterator) {
        DataRow &target = iterator.next();
        for (uint32_t i = 0; i < header_.size(); ++i) {
            target[i] = header_[i];
        }
        for (uint32_t i = 0; i < fields_.size(); ++i) {
            fields_[i]->dump(target, i + header_.size());
        }
    }

    AggRecordingReducer::AggRecordingReducer(uint32_t numHeaders, AggRecordingField *field)
            : AggReducer(numHeaders, {field}), field_(unique_ptr<AggRecordingField>(field)) {}

    uint32_t AggRecordingReducer::size() {
        return field_->keys().size();
    }

    void AggRecordingReducer::dump(DataRowIterator &iterator) {
        auto keys = field_->keys();
        for (auto &key: keys) {
            DataRow &target = iterator.next();
            for (uint32_t i = 0; i < header_.size(); ++i) {
                target[i] = header_[i];
            }
            field_->dump(target, header_.size());
            target[header_.size() + 1] = key;
        }
    }

    AggField::AggField(uint32_t index) : readIndex_(index) {}

    AggRecordingField::AggRecordingField(uint32_t rIndex, uint32_t kIndex)
            : AggField(rIndex), keyIndex_(kIndex) {}

    vector<int32_t> &AggRecordingField::keys() {
        return keys_;
    }

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
        Max<T, ACC>::Max(uint32_t rIndex) : AggField(rIndex), value_(INT_MIN) {}

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
        Min<T, ACC>::Min(uint32_t rIndex) : AggField(rIndex), value_(INT_MAX) {}

        template<typename T, typename ACC>
        void Min<T, ACC>::merge(AggField &another) {
            value_ = std::min(value_, static_cast<Min<T, ACC> &>(another).value_);
        }

        template<typename T, typename ACC>
        void Min<T, ACC>::reduce(DataRow &input) {
            value_ = std::min(value_, ACC::get(input[readIndex_]));
        }

        template<typename T, typename ACC>
        void Min<T, ACC>::dump(DataRow &output, uint32_t index) {
            output[index] = value_;
        }

        template<typename T, typename ACC>
        RecordingMin<T, ACC>::RecordingMin(uint32_t vIndex, uint32_t kIndex)
                : AggRecordingField(vIndex, kIndex), value_(INT_MAX) {}

        template<typename T, typename ACC>
        void RecordingMin<T, ACC>::merge(AggField &another) {
            auto arm = static_cast<RecordingMin<T, ACC> &>(another);
            if (value_ > arm.value_) {
                value_ = arm.value_;
                keys_ = arm.keys_;
            } else if (value_ == arm.value_) {
                keys_.insert(keys_.end(), arm.keys_.begin(), arm.keys_.end());
            }
        }

        template<typename T, typename ACC>
        void RecordingMin<T, ACC>::reduce(DataRow &input) {
            auto newval = ACC::get(input[readIndex_]);
            if (newval < value_) {
                value_ = newval;
                keys_.clear();
                keys_.push_back(input[keyIndex_].asInt());
            } else if (newval == value_) {
                keys_.push_back(input[keyIndex_].asInt());
            }
        }

        template<typename T, typename ACC>
        void RecordingMin<T, ACC>::dump(DataRow &output, uint32_t index) {
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

        template
        class Sum<int32_t, AsInt>;

        template
        class Sum<double, AsDouble>;

        template
        class Max<int32_t, AsInt>;

        template
        class Max<double, AsDouble>;

        template
        class Min<int32_t, AsInt>;

        template
        class Min<double, AsDouble>;

        template
        class RecordingMin<int32_t, AsInt>;

        template
        class RecordingMin<double, AsDouble>;

        template
        class Avg<int32_t, AsInt>;

        template
        class Avg<double, AsDouble>;
    }

    HashCore::HashCore(function<uint64_t(DataRow &)> hasher,
                       function<unique_ptr<AggReducer>(DataRow &)> headerInit)
            : hasher_(hasher), headerInit_(headerInit) {}

    HashCore::~HashCore() {
        container_.clear();
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

    void HashCore::dump(MemTable &table) {
        uint32_t size = 0;
        for (auto &item: container_) {
            size += item.second->size();
        }
        auto block = table.allocate(size);

        auto it = container_.begin();
        auto wit = block->rows();
        for (uint32_t i = 0; i < container_.size(); ++i) {
            it->second->dump((*wit));
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

    void HashCore::translate(function<void(DataField &, uint32_t)> translator) {

    }

    TableCore::TableCore(uint32_t tableSize, function<uint32_t(DataRow &)> indexer,
                         function<unique_ptr<AggReducer>(DataRow &)> headerInit)
            : container_(tableSize), indexer_(indexer), headerInit_(headerInit) {}

    TableCore::~TableCore() {
        container_.clear();
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
            if (container_[i]) {
                container_[i]->merge(*(another.container_[i]));
            } else {
                container_[i] = move(another.container_[i]);
            }
        }
    }

    void TableCore::dump(MemTable &table) {
        uint32_t size = 0;
        for (uint32_t i = 0; i < container_.size(); ++i) {
            if (container_[i].get()) {
                size += container_[i]->size();
            }
        }
        auto block = table.allocate(size);
        auto rows = block->rows();
        for (uint32_t i = 0; i < container_.size(); ++i) {
            if (container_[i].get()) {
                container_[i]->dump((*rows));
            }
        }
    }

    SimpleCore::SimpleCore(function<unique_ptr<AggReducer>(DataRow &)> headerInit)
            : headerInit_(headerInit) {}

    void SimpleCore::consume(DataRow &row) {
        if (__builtin_expect(reducer_ == nullptr, 0)) {
            reducer_ = headerInit_(row);
        }
        reducer_->reduce(row);
    }

    void SimpleCore::reduce(SimpleCore &another) {
        reducer_->merge(*(another.reducer_));
    }

    void SimpleCore::dump(MemTable &table) {
        auto block = table.allocate(reducer_->size());
        reducer_->dump((*block->rows()));
    }


    template<typename CORE>
    Agg<CORE>::Agg(const vector<uint32_t> &col_size, function<unique_ptr<CORE>()> coreMaker, bool vertical)
            : output_col_size_(col_size), vertical_(vertical), coreMaker_(coreMaker) {}

    template<typename CORE>
    Agg<CORE>::Agg(uint32_t num_fields, function<unique_ptr<CORE>()> coreMaker, bool vertical)
            : Agg(lqf::colSize(num_fields), coreMaker, vertical) {}


    template<typename CORE>
    shared_ptr<Table> Agg<CORE>::agg(Table &input) {
        function<unique_ptr<CORE>(
                const shared_ptr<Block> &)> mapper = bind(&Agg::processBlock, this, _1);

        function<unique_ptr<CORE>(unique_ptr<CORE> &, unique_ptr<CORE> &)> reducer =
                [](unique_ptr<CORE> &a, unique_ptr<CORE> &b) {
                    a->reduce(*b);
                    return move(a);
                };
        auto merged = input.blocks()->map(mapper)->reduce(reducer);

        auto result = MemTable::Make(output_col_size_, vertical_);
        merged->dump(*result);

        return result;
    }

    template<typename CORE>
    unique_ptr<CORE> Agg<CORE>::processBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto core = coreMaker_();
        uint64_t blockSize = block->size();

        for (uint32_t i = 0; i < blockSize; ++i) {
            core->consume(rows->next());
        }
        return core;
    }

    template<typename CORE>
    DictAgg<CORE>::DictAgg(const vector<uint32_t> &col_size, function<unique_ptr<CORE>()> coreMaker,
                           initializer_list<uint32_t> need_trans, bool vertical)
            : Agg<CORE>(col_size, coreMaker, vertical), need_trans_(need_trans) {}

    using namespace std::placeholders;

    template<typename CORE>
    unique_ptr<CORE> DictAgg<CORE>::processBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto core = this->coreMaker_();
        uint64_t blockSize = block->size();

        for (uint32_t i = 0; i < blockSize; ++i) {
            core->consume(rows->next());
        }
        auto translator = bind(&DataRowIterator::translate, rows.get(), _1, _2, _3);
        core->translate(need_trans_, translator);

        return core;
    }

    template
    class Agg<HashCore>;

    template
    class Agg<TableCore>;

    template
    class Agg<SimpleCore>;

    template
    class DictAgg<HashCore>;

    template
    class DictAgg<TableCore>;

    template
    class DictAgg<SimpleCore>;
}
