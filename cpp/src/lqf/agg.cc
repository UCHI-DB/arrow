//
// Created by harper on 2/21/20.
//

#include <climits>
#include <limits>
//#include <boost/container_hash/hash.hpp>
#include "agg.h"

using namespace std;
using namespace std::placeholders;
namespace lqf {

    namespace agg {
        AggReducer::AggReducer(uint32_t numFields, vector<AggField *> fields) :
                AggReducer(lqf::colOffset(numFields), fields) {}


        AggReducer::AggReducer(const vector<uint32_t> &col_offset, vector<AggField *> fields)
                : storage_(col_offset) {
            header_size_ = col_offset.size() - 1 - fields.size();
            auto start = header_size_;
            for (auto &field:fields) {
                field->storage_ = storage_[start++].data();
                field->init();
                fields_.push_back(unique_ptr<AggField>(field));
            }
        }

        void AggReducer::reduce(DataRow &row) {
            for (auto &field: fields_) {
                field->reduce(row);
            }
        }

        void AggReducer::merge(AggReducer &reducer) {
            auto field_size = fields_.size();
            for (uint32_t i = 0; i < field_size; ++i) {
                this->fields_[i]->merge(*reducer.fields_[i]);
            }
        }

        void AggReducer::dump(MemDataRow &target) {
            for (auto &field:fields_) {
                field->dump();
            }
            target = storage_;
        }

        AggRecordingReducer::AggRecordingReducer(vector<uint32_t> &col_offset, AggRecordingField *field)
                : AggReducer(col_offset, {field}), field_(field) {}

        uint32_t AggRecordingReducer::size() {
            return field_->keys().size();
        }

        void AggRecordingReducer::dump(MemDataRow &target) {
            for (auto &field:fields_) {
                field->dump();
            }
            target = storage_;
            auto key = field_->keys().back();
            field_->keys().pop_back();
            target[header_size_ - 1] = key;
        }

        AggField::AggField(uint32_t index) : readIndex_(index) {
            storage_.size_ = 1;
        }

        AggRecordingField::AggRecordingField(uint32_t rIndex, uint32_t kIndex)
                : AggField(rIndex), keyIndex_(kIndex) {}

        vector<int32_t> &AggRecordingField::keys() {
            return keys_;
        }

        template<typename T, typename ACC>
        Sum<T, ACC>::Sum(uint32_t rIndex) : AggField(rIndex) {}

        template<typename T, typename ACC>
        void Sum<T, ACC>::init() {
            value_ = (T *) storage_.pointer_.raw_;
            *value_ = 0;
        }

        template<typename T, typename ACC>
        void Sum<T, ACC>::merge(AggField &another) {
            *value_ += *(static_cast<Sum<T, ACC> &>(another).value_);
        }

        template<typename T, typename ACC>
        void Sum<T, ACC>::reduce(DataRow &input) {
            *value_ += ACC::get(input[readIndex_]);
        }

        template<typename T, typename ACC>
        Max<T, ACC>::Max(uint32_t rIndex) : AggField(rIndex) {}

        template<typename T, typename ACC>
        void Max<T, ACC>::init() {
            value_ = (T *) storage_.pointer_.raw_;
            *value_ = INT32_MIN;
        }

        template<typename T, typename ACC>
        void Max<T, ACC>::merge(AggField &another) {
            *value_ = std::max(*value_, *static_cast<Max<T, ACC> &>(another).value_);
        }

        template<typename T, typename ACC>
        void Max<T, ACC>::reduce(DataRow &input) {
            *value_ = std::max(*value_, ACC::get(input[readIndex_]));
        }

        template<typename T, typename ACC>
        Min<T, ACC>::Min(uint32_t rIndex) : AggField(rIndex) {}

        template<typename T, typename ACC>
        void Min<T, ACC>::init() {
            value_ = (T *) storage_.pointer_.raw_;
            *value_ = INT32_MAX;
        }

        template<typename T, typename ACC>
        void Min<T, ACC>::merge(AggField &another) {
            *value_ = std::min(*value_, *static_cast<Min<T, ACC> &>(another).value_);
        }

        template<typename T, typename ACC>
        void Min<T, ACC>::reduce(DataRow &input) {
            *value_ = std::min(*value_, ACC::get(input[readIndex_]));
        }

        template<typename T, typename ACC>
        RecordingMin<T, ACC>::RecordingMin(uint32_t vIndex, uint32_t kIndex)
                : AggRecordingField(vIndex, kIndex) {}

        template<typename T, typename ACC>
        void RecordingMin<T, ACC>::init() {
            value_ = (T *) storage_.pointer_.raw_;
            *value_ = INT32_MAX;
        }

        template<typename T, typename ACC>
        void RecordingMin<T, ACC>::merge(AggField &another) {
            auto arm = static_cast<RecordingMin<T, ACC> &>(another);
            if (*value_ > *arm.value_) {
                *value_ = *arm.value_;
                keys_ = arm.keys_;
            } else if (*value_ == *arm.value_) {
                keys_.insert(keys_.end(), arm.keys_.begin(), arm.keys_.end());
            }
        }

        template<typename T, typename ACC>
        void RecordingMin<T, ACC>::reduce(DataRow &input) {
            auto newval = ACC::get(input[readIndex_]);
            if (newval < *value_) {
                *value_ = newval;
                keys_.clear();
                keys_.push_back(input[keyIndex_].asInt());
            } else if (newval == *value_) {
                keys_.push_back(input[keyIndex_].asInt());
            }
        }

        template<typename T, typename ACC>
        RecordingMax<T, ACC>::RecordingMax(uint32_t vIndex, uint32_t kIndex)
                : AggRecordingField(vIndex, kIndex) {}

        template<typename T, typename ACC>
        void RecordingMax<T, ACC>::init() {
            value_ = (T *) storage_.pointer_.raw_;
            *value_ = INT32_MIN;
        }

        template<typename T, typename ACC>
        void RecordingMax<T, ACC>::merge(AggField &another) {
            auto arm = static_cast<RecordingMax<T, ACC> &>(another);
            if (*value_ < *arm.value_) {
                *value_ = *arm.value_;
                keys_ = arm.keys_;
            } else if (value_ == arm.value_) {
                keys_.insert(keys_.end(), arm.keys_.begin(), arm.keys_.end());
            }
        }

        template<typename T, typename ACC>
        void RecordingMax<T, ACC>::reduce(DataRow &input) {
            auto newval = ACC::get(input[readIndex_]);
            if (newval > *value_) {
                *value_ = newval;
                keys_.clear();
                keys_.push_back(input[keyIndex_].asInt());
            } else if (newval == *value_) {
                keys_.push_back(input[keyIndex_].asInt());
            }
        }

        template<typename T, typename ACC>
        Avg<T, ACC>::Avg(uint32_t rIndex) : AggField(rIndex), value_(0), count_(0) {}

        template<typename T, typename ACC>
        void Avg<T, ACC>::init() {}

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
        void Avg<T, ACC>::dump() {
            storage_ = static_cast<double>(value_) / count_;
        }

        template<typename T, typename ACC>
        DistinctCount<T, ACC>::DistinctCount(uint32_t rIndex): AggField(rIndex) {}

        template<typename T, typename ACC>
        void DistinctCount<T, ACC>::init() {}

        template<typename T, typename ACC>
        void DistinctCount<T, ACC>::reduce(DataRow &input) {
            values_.insert(ACC::get(input[readIndex_]));
        }

        template<typename T, typename ACC>
        void DistinctCount<T, ACC>::merge(AggField &another) {
            auto to_insert = static_cast<DistinctCount<T, ACC> &>(another).values_;
            values_.insert(to_insert.begin(), to_insert.end());
        }

        template<typename T, typename ACC>
        void DistinctCount<T, ACC>::dump() {
            storage_ = static_cast<int32_t>(values_.size());
        }

        Count::Count() : AggField(-1) {}

        void Count::init() {
            count_ = storage_.pointer_.ival_;
            *count_ = 0;
        }

        void Count::merge(AggField &another) {
            *count_ += *static_cast<Count &>(another).count_;
        }

        void Count::reduce(DataRow &input) {
            *count_ += 1;
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
        class RecordingMax<int32_t, AsInt>;

        template
        class RecordingMax<double, AsDouble>;

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

        template
        class DistinctCount<int32_t, AsInt>;
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

    void HashCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
        // Use an init size
        uint32_t size = container_.size();
        auto block = table.allocate(size);

        MemDataRow buffer(table.colOffset());

        auto wit = block->rows();

        uint32_t counter = 0;
        uint32_t remain = size;
        for (auto &pair: container_) {
            uint32_t item_size = pair.second->size();
            if (__builtin_expect(item_size > remain, 0)) {
                remain += size;
                size *= 2;
                block->resize(size);
            }

            uint32_t written = 0;
            for (uint32_t i = 0; i < item_size; ++i) {
                pair.second->dump(buffer);
                if (__builtin_expect(!pred || pred(buffer), 1)) {
                    DataRow &nextrow = wit->next();
                    // Copy data
                    nextrow = buffer;
                    ++written;
                }
            }
            counter += written;
            remain -= written;
        }
        if (size != counter)
            block->resize(counter);
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
        auto container_size = container_.size();
        for (uint32_t i = 0; i < container_size; ++i) {
            if (container_[i]) {
                container_[i]->merge(*(another.container_[i]));
            } else {
                container_[i] = move(another.container_[i]);
            }
        }
    }

    void TableCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
        // Use an init size
        uint32_t size = container_.size();
        auto block = table.allocate(size);

        MemDataRow buffer(table.colOffset());

        auto wit = block->rows();

        uint32_t counter = 0;
        uint32_t remain = size;
        for (auto &item: container_) {
            if (item) {
                uint32_t item_size = item->size();
                if (__builtin_expect(item_size > remain, 0)) {
                    remain += size;
                    size *= 2;
                    block->resize(size);
                }

                uint32_t written = 0;
                for (uint32_t i = 0; i < item_size; ++i) {
                    item->dump(buffer);
                    if (__builtin_expect(!pred || pred(buffer), 1)) {
                        DataRow &nextrow = wit->next();
                        // Copy data
                        nextrow = buffer;
                        ++written;
                    }
                }
                counter += written;
                remain -= written;
            }
        }
        if (size != counter)
            block->resize(counter);
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
        if (another.reducer_) {
            if (reducer_) {
                reducer_->merge(*(another.reducer_));
            } else {
                reducer_ = move(another.reducer_);
            }
        }
    }

    void SimpleCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
        // Use an init size
        uint32_t size = reducer_->size();
        auto block = table.allocate(size);

        MemDataRow buffer(table.colOffset());

        auto wit = block->rows();

        uint32_t written = 0;
        for (uint32_t i = 0; i < size; ++i) {
            reducer_->dump(buffer);
            if (__builtin_expect(!pred || pred(buffer), 1)) {
                DataRow &nextrow = wit->next();
                // Copy data
                nextrow = buffer;
                ++written;
            }
        }
        if (size != written)
            block->resize(written);
    }

    template<typename CORE>
    Agg<CORE>::Agg(const vector<uint32_t> &col_size, bool vertical)
            : Node(1), output_col_size_(col_size), vertical_(vertical), predicate_(nullptr) {}

    template<typename CORE>
    unique_ptr<NodeOutput> Agg<CORE>::execute(const vector<NodeOutput *> &input) {
        auto input0 = static_cast<TableOutput *>(input[0]);
        auto result = agg(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(result));
    }

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
        merged->dump(*result, predicate_);

        return result;
    }

    template<typename CORE>
    unique_ptr<CORE> Agg<CORE>::processBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto core = makeCore();
        uint64_t blockSize = block->size();

        for (uint32_t i = 0; i < blockSize; ++i) {
            core->consume(rows->next());
        }
        return core;
    }

    // Subclass should override this
    template<typename CORE>
    unique_ptr<CORE> Agg<CORE>::makeCore() { return nullptr; }

    template<typename CORE>
    void Agg<CORE>::useVertical() { vertical_ = true; }

    template<typename CORE>
    void Agg<CORE>::setPredicate(function<bool(DataRow &)> pred) {
        predicate_ = pred;
    }


    CoreMaker::CoreMaker(const vector<uint32_t> &output_col_size, const initializer_list<int32_t> &header_fields,
                         function<vector<AggField *>()> agg_fields)
            : output_col_size_(output_col_size), agg_fields_(agg_fields) {
        output_offset_.push_back(0);
        for (auto col_size: output_col_size) {
            output_offset_.push_back(output_offset_.back() + col_size);
        }

        uint32_t i = 0;
        for (auto &inst: header_fields) {
            switch (inst >> 16) {
                case 0:
                    // INT
                    int_fields_.push_back(pair<uint32_t, uint32_t>(i, inst & 0xFFFF));
                    break;
                case 1:
                    // DOUBLE
                    double_fields_.push_back(pair<uint32_t, uint32_t>(i, inst & 0xFFFF));
                    break;
                case 2:
                    // ByteArray
                    bytearray_fields_.push_back(pair<uint32_t, uint32_t>(i, inst & 0xFFFF));
                    break;
                case 3:
                    // Raw
                    raw_fields_.push_back(pair<uint32_t, uint32_t>(i, inst & 0xFFFF));
                    break;
                default:
                    break;
            }
            ++i;
        }
    }

    function<unique_ptr<AggReducer>(DataRow &)> CoreMaker::headerInit() {
        return bind(useRecording_ ? &CoreMaker::initRecordingHeader
                                  : &CoreMaker::initHeader, this, placeholders::_1);
    }

    unique_ptr<AggReducer> CoreMaker::initHeader(DataRow &row) {
        unique_ptr<AggReducer> reducer = unique_ptr<AggReducer>(
                new AggReducer(output_offset_, agg_fields_()));
        for (auto &item : int_fields_) {
            reducer->storage()[item.first] = row[item.second].asInt();
        }
        for (auto &item: double_fields_) {
            reducer->storage()[item.first] = row[item.second].asDouble();
        }
        for (auto &item: bytearray_fields_) {
            reducer->storage()[item.first] = row[item.second].asByteArray();
        }
        for (auto &item : raw_fields_) {
            reducer->storage()[item.first] = row(item.second).asInt();
        }
        return reducer;
    }

    unique_ptr<AggReducer> CoreMaker::initRecordingHeader(DataRow &row) {
        unique_ptr<AggReducer> reducer = unique_ptr<AggReducer>(
                new AggRecordingReducer(output_offset_, dynamic_cast<AggRecordingField *>(agg_fields_()[0])));
        for (auto &item : int_fields_) {
            reducer->storage()[item.first] = row[item.second].asInt();
        }
        for (auto &item: double_fields_) {
            reducer->storage()[item.first] = row[item.second].asDouble();
        }
        for (auto &item: bytearray_fields_) {
            reducer->storage()[item.first] = row[item.second].asByteArray();
        }
        for (auto &item : raw_fields_) {
            reducer->storage()[item.first] = row(item.second).asInt();
        }
        return reducer;
    }

    void CoreMaker::useRecording() {
        useRecording_ = true;
    }

    HashAgg::HashAgg(const vector<uint32_t> &col_size,
                     const initializer_list<int32_t> &header_fields, function<vector<AggField *>()> agg_fields,
                     function<uint64_t(DataRow &)> hasher, bool vertical)
            : Agg<HashCore>(col_size, vertical), CoreMaker(col_size, header_fields, agg_fields), hasher_(hasher) {}

    unique_ptr<HashCore> HashAgg::makeCore() {
        return unique_ptr<HashCore>(new HashCore(hasher_, headerInit()));
    }

    TableAgg::TableAgg(const vector<uint32_t> &col_size,
                       const initializer_list<int32_t> &header_fields, function<vector<AggField *>()> agg_fields,
                       uint32_t table_size, function<uint32_t(DataRow &)> indexer)
            : Agg<TableCore>(col_size), CoreMaker(col_size, header_fields, agg_fields),
              table_size_(table_size), indexer_(indexer) {}

    unique_ptr<TableCore> TableAgg::makeCore() {
        return unique_ptr<TableCore>(new TableCore(table_size_, indexer_, headerInit()));
    }

    SimpleAgg::SimpleAgg(const vector<uint32_t> &col_size, function<vector<AggField *>()> agg_fields) :
            Agg<SimpleCore>(col_size), CoreMaker(col_size, {}, agg_fields) {}

    unique_ptr<SimpleCore> SimpleAgg::makeCore() {
        return unique_ptr<SimpleCore>(new SimpleCore(headerInit()));
    }

    template
    class Agg<HashCore>;

    template
    class Agg<TableCore>;

    template
    class Agg<SimpleCore>;

}
