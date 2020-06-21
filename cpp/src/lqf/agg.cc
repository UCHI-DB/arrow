//
// Created by Harper on 6/17/20.
//

#include "agg.h"

namespace lqf {
    using namespace rowcopy;
    namespace agg {

        double AsDouble::MAX = INT32_MAX;
        double AsDouble::MIN = INT32_MIN;
        int32_t AsInt::MAX = INT32_MAX;
        int32_t AsInt::MIN = INT32_MIN;

        AggField::AggField(uint32_t size, uint32_t read_idx, bool need_dump)
                : size_(size), read_idx_(read_idx), need_dump_(need_dump) {}

        void AggField::attach(DataRow &target) {
            value_ = target.raw() + write_idx_;
        }

        void AggField::dump() { // Do nothing by default
        }

        void AggField::init() {
            value_ = 0;
        }

        Count::Count() : AggField(1, 0) {}

        void Count::reduce(DataRow &input) {
            *value_.pointer_.ival_ += 1;
        }

        void Count::merge(AggField &another) {
            *value_.pointer_.ival_ += static_cast<Count &>(another).value_.asInt();
        }

        IntDistinctCount::IntDistinctCount(uint32_t read_idx)
                : AggField(1, read_idx, true) {}

        void IntDistinctCount::attach(DataRow &target) {
            AggField::attach(target);
            distinct_ = reinterpret_cast<unordered_set<int32_t> *>(*value_.pointer_.raw_);
        }

        void IntDistinctCount::init() {
            distinct_ = new unordered_set<int32_t>();
            *value_.pointer_.raw_ = reinterpret_cast<uint64_t>(distinct_);
        }

        void IntDistinctCount::reduce(DataRow &input) {
            distinct_->insert(input[read_idx_].asInt());
        }

        void IntDistinctCount::merge(AggField &another) {
            auto &a = static_cast<IntDistinctCount &>(another);
            distinct_->insert(a.distinct_->begin(), a.distinct_->end());
            delete a.distinct_;
        }

        void IntDistinctCount::dump() {
            auto size = distinct_->size();
            delete distinct_;
            value_ = static_cast<int32_t>(size);
        }

        IntSum::IntSum(uint32_t read_idx) : AggField(1, read_idx) {}

        void IntSum::reduce(DataRow &input) {
            *value_.pointer_.ival_ += input[read_idx_].asInt();
        }

        void IntSum::merge(AggField &another) {
            *value_.pointer_.ival_ += static_cast<IntSum &>(another).value_.asInt();
        }

        DoubleSum::DoubleSum(uint32_t read_idx) : AggField(1, read_idx) {}

        void DoubleSum::reduce(DataRow &input) {
            *value_.pointer_.dval_ += input[read_idx_].asDouble();
        }

        void DoubleSum::merge(AggField &another) {
            *value_.pointer_.dval_ += static_cast<DoubleSum &>(another).value_.asDouble();
        }

        template<typename ACC>
        Avg<ACC>::Avg(uint32_t read_idx) : AggField(2, read_idx, true) {}

        template<typename ACC>
        void Avg<ACC>::attach(DataRow &target) {
            AggField::attach(target);
            count_ = target.raw() + write_idx_ + 1;
        }

        template<typename ACC>
        void Avg<ACC>::reduce(DataRow &input) {
            value_ = ACC::get(value_) + ACC::get(input[read_idx_]);
            *count_.pointer_.ival_ += 1;
        }

        template<typename ACC>
        void Avg<ACC>::merge(AggField &another) {
            Avg<ACC> &anotherIa = static_cast<Avg<ACC> &>(another);
            value_ = ACC::get(value_) + ACC::get(anotherIa.value_);
            *count_.pointer_.ival_ += anotherIa.count_.asInt();
        }

        template<typename ACC>
        void Avg<ACC>::dump() {
            auto value = ACC::get(value_);
            auto count = count_.asInt();
            value_ = static_cast<double>(value) / count;
        }

        template<typename ACC>
        void Avg<ACC>::init() {
            AggField::init();
            count_ = 0;
        }

        template<typename ACC>
        Max<ACC>::Max(uint32_t read_idx) : AggField(1, read_idx) {}

        template<typename ACC>
        void Max<ACC>::init() {
            value_ = ACC::MIN;
        }

        template<typename ACC>
        void Max<ACC>::reduce(DataRow &input) {
            value_ = std::max(ACC::get(value_), ACC::get(input[read_idx_]));
        }

        template<typename ACC>
        void Max<ACC>::merge(AggField &another) {
            value_ = std::max(ACC::get(value_), ACC::get(static_cast<Max<ACC> &>(another).value_));
        }

        template<typename ACC>
        Min<ACC>::Min(uint32_t read_idx) : AggField(1, read_idx) {}

        template<typename ACC>
        void Min<ACC>::init() {
            value_ = ACC::MAX;
        }

        template<typename ACC>
        void Min<ACC>::reduce(DataRow &input) {
            value_ = std::min(ACC::get(value_), ACC::get(input[read_idx_]));
        }

        template<typename ACC>
        void Min<ACC>::merge(AggField &another) {
            value_ = std::min(ACC::get(value_), ACC::get(static_cast<Min<ACC> &>(another).value_));
        }

        AggReducer::AggReducer(Snapshoter *header_copier,
                               function<void(DataRow &, DataRow &)> *row_copier,
                               vector<AggField *> fields,
                               const vector<uint32_t> &fields_offset)
                : header_copier_(header_copier), row_copier_(row_copier) {
            int i = 0;
            for (auto &field: fields) {
                fields_.push_back(unique_ptr<AggField>(field));
                field->write_at(fields_offset[i++]);
            }
        }

        AggReducer::AggReducer(Snapshoter *header_copier,
                               function<void(DataRow &, DataRow &)> *row_copier,
                               AggField *field,
                               uint32_t field_offset)
                : header_copier_(header_copier), row_copier_(row_copier) {
            fields_.push_back(unique_ptr<AggField>(field));
            field->write_at(field_offset);
        }

        void AggReducer::attach(DataRow &target) {
            storage_ = &target;
            // Attach each field
            for (auto &field: fields_) {
                field->attach(target);
            }
        }

        void AggReducer::init(DataRow &input) {
            (*header_copier_)(*storage_, input);
            // Init the storage content as 0
            for (auto &field:fields_) {
                field->init();
            }
            reduce(input);
        }


        void AggReducer::reduce(DataRow &input) {
            for (auto &field: fields_) {
                field->reduce(input);
            }
        }

        void AggReducer::dump() {
            for (auto &field: fields_) {
                field->dump();
            }
        }

        void AggReducer::merge(AggReducer &another) {
            for (auto i = 0u; i < fields_.size(); ++i) {
                fields_[i]->merge(*another.fields_[i]);
            }
        }

        void AggReducer::assign(AggReducer &another) {
            (*row_copier_)(*storage_, *another.storage_);
        }

        HashCore::HashCore(const vector<uint32_t> &col_offset, unique_ptr<AggReducer> reducer,
                           function<uint64_t(DataRow &)> &hasher, bool need_dump)
                : reducer_(move(reducer)), hasher_(hasher), map_(col_offset), need_dump_(need_dump) {}

        void HashCore::reduce(DataRow &row) {
            auto key = hasher_(row);
            DataRow *exist = map_.find(key);
            if (exist) {
                reducer_->attach(*exist);
                reducer_->reduce(row);
            } else {
                reducer_->attach(map_.insert(key));
                reducer_->init(row);
            }
        }

        void HashCore::merge(HashCore &another) {
            auto ite = another.map_.map_iterator();
            while (ite->hasNext()) {
                auto &next = ite->next();
                another.reducer_->attach(next.second);
                auto exist = map_.find(next.first);
                if (exist) {
                    reducer_->attach(*exist);
                    reducer_->merge(*another.reducer_);
                } else {
                    DataRow &inserted = map_.insert(next.first);
                    reducer_->attach(inserted);
                    reducer_->assign(*another.reducer_);
                }
            }
        }

        void HashCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            auto flexblock = table.allocateFlex();
            if (!pred) {
                if (need_dump_) {
                    auto iterator = map_.iterator();
                    while (iterator->hasNext()) {
                        DataRow &next = iterator->next();
                        reducer_->attach(next);
                        reducer_->dump();
                    }
                }
                flexblock->assign(map_.memory(), map_.size());
            } else {
                auto ite = map_.iterator();
                auto row_copier = reducer_->row_copier();
                if (need_dump_) {
                    while (ite->hasNext()) {
                        DataRow &next = ite->next();
                        reducer_->attach(next);
                        reducer_->dump();
                        if (pred(next)) {
                            DataRow &write = flexblock->push_back();
                            (*row_copier)(write, next);
                        }
                    }
                } else {
                    while (ite->hasNext()) {
                        DataRow &next = ite->next();
                        if (pred(next)) {
                            DataRow &write = flexblock->push_back();
                            (*row_copier)(write, next);
                        }
                    }
                }
            }
        }

        SimpleCore::SimpleCore(const vector<uint32_t> &col_offset, unique_ptr<AggReducer> reducer, bool need_dump)
                : reducer_(move(reducer)), storage_(col_offset), need_dump_(need_dump) {
            reducer_->attach(storage_);
            for (auto &field:reducer_->fields()) {
                field->init();
            }
        }

        void SimpleCore::reduce(DataRow &row) {
            reducer_->reduce(row);
        }

        void SimpleCore::merge(SimpleCore &another) {
            reducer_->merge(*another.reducer_);
        }

        void SimpleCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
            // Ignore predicate
            if (need_dump_) {
                reducer_->dump();
            }
            auto block = table.allocate(1);
            auto row = block->rows();
            (*reducer_->row_copier())(row->next(), storage_);
        }

        namespace recording {

            RecordingAggField::RecordingAggField(uint32_t read_idx, uint32_t key_idx)
                    : AggField(2, read_idx), key_idx_(key_idx) {}

            void RecordingAggField::attach(DataRow &input) {
                AggField::attach(input);
                keys_ = reinterpret_cast<unordered_set<int32_t> *>(*(this->value_.data() + 1));
            }

            void RecordingAggField::init() {
                keys_ = new unordered_set<int32_t>();
                *(this->value_.data() + 1) = reinterpret_cast<uint64_t>(keys_);
            }

            void RecordingAggField::merge(AggField &another) {
                auto &arf = static_cast<RecordingAggField &>(another);
                delete arf.keys_;
            }

            template<typename ACC>
            RecordingMin<ACC>::RecordingMin(uint32_t read_idx, uint32_t key_idx)
                    : RecordingAggField(read_idx, key_idx) {}

            template<typename ACC>
            void RecordingMin<ACC>::init() {
                RecordingAggField::init();
                value_ = ACC::MAX;
            }

            template<typename ACC>
            void RecordingMin<ACC>::reduce(DataRow &input) {
                auto newval = ACC::get(input[read_idx_]);
                auto current = ACC::get(value_);
                if (newval < current) {
                    keys_->clear();
                    keys_->insert(input[key_idx_].asInt());
                    value_ = newval;
                } else if (newval == current) {
                    keys_->insert(input[key_idx_].asInt());
                }
            }

            template<typename ACC>
            void RecordingMin<ACC>::merge(AggField &a) {
                RecordingMin<ACC> &another = static_cast<RecordingMin<ACC> &>(a);
                auto myvalue = ACC::get(value_);
                auto othervalue = ACC::get(another.value_);

                if (myvalue > othervalue) {
                    keys_->clear();
                }
                if (myvalue >= othervalue) {
                    value_ = othervalue;
                    keys_->insert(another.keys_->begin(), another.keys_->end());
                }
                RecordingAggField::merge(a);
            }

            template<typename ACC>
            RecordingMax<ACC>::RecordingMax(uint32_t read_idx, uint32_t key_idx)
                    : RecordingAggField(read_idx, key_idx) {}

            template<typename ACC>
            void RecordingMax<ACC>::init() {
                RecordingAggField::init();
                value_ = ACC::MIN;
            }

            template<typename ACC>
            void RecordingMax<ACC>::reduce(DataRow &input) {
                auto newval = ACC::get(input[read_idx_]);
                auto current = ACC::get(value_);
                if (newval > current) {
                    keys_->clear();
                    keys_->insert(input[key_idx_].asInt());
                    value_ = newval;
                } else if (newval == current) {
                    keys_->insert(input[key_idx_].asInt());
                }
            }

            template<typename ACC>
            void RecordingMax<ACC>::merge(AggField &a) {
                RecordingMax<ACC> &another = static_cast<RecordingMax<ACC> &>(a);
                auto myvalue = ACC::get(value_);
                auto othervalue = ACC::get(another.value_);

                if (myvalue < othervalue) {
                    keys_->clear();
                }
                if (myvalue <= othervalue) {
                    value_ = othervalue;
                    keys_->insert(another.keys_->begin(), another.keys_->end());
                }
                RecordingAggField::merge(a);
            }

//            RecordingAggReducer::RecordingAggReducer(Snapshoter *header_copier,
//                                                     function<void(DataRow &, DataRow &)> *row_copier,
//                                                     RecordingAggField *field, uint32_t field_start)
//                    : AggReducer(header_copier, row_copier, vector<AggField *>{field}, vector<uint32_t>{field_start}) {
//                field_ = static_cast<RecordingAggField *>(fields_[0].get());
//            }
//
//            void RecordingAggReducer::init(DataRow &input) {
//                (*header_copier_)(*storage_, input);
//                auto keyholder = make_shared<vector<int32_t>>();
//                keys_.emplace_back(move(keyholder));
//                field_->init();
//                field_->associate(keys_.back().get());
//                reduce(input);
//            }
//
//            void RecordingAggReducer::assign(RecordingAggReducer &another) {
//                // Assign happens when the storage is not init
//                keys_.push_back(make_shared<vector<int32_t>>());
//                auto keys = keys_.back().get();
//                auto tomove = another.field_->keys();
//                AggReducer::assign(another);
//                (*keys) = move(*tomove);
//                field_->associate(keys);
//            }

            RecordingHashCore::RecordingHashCore(const vector<uint32_t> &col_offset,
                                                 unique_ptr<AggReducer> reducer,
                                                 function<uint64_t(DataRow &)> &hasher)
                    : HashCore(col_offset, move(reducer), hasher, false),
                      write_key_index_(col_offset.back() - 1) {}

            void RecordingHashCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
                auto flex = table.allocateFlex();
                auto copier = reducer_->row_copier();
                auto field = static_cast<RecordingAggField *>(reducer_->fields()[0].get());
                if (!pred) {
                    auto iterator = map_.iterator();
                    while (iterator->hasNext()) {
                        DataRow &next = iterator->next();
                        reducer_->attach(next);
                        auto keys = field->keys();
                        for (auto &key: *keys) {
                            DataRow &writeto = flex->push_back();
                            (*copier)(writeto, next);
                            writeto[write_key_index_] = key;
                        }
                        delete keys;
                    }
                } else {
                    MemDataRow buffer(table.colOffset());
                    auto iterator = map_.iterator();
                    while (iterator->hasNext()) {
                        DataRow &next = iterator->next();
                        reducer_->attach(next);
                        auto keys = field->keys();
                        for (auto &key: *keys) {
                            (*copier)(buffer, next);
                            buffer[write_key_index_] = key;
                            if (pred(buffer)) {
                                DataRow &writeto = flex->push_back();
                                (*copier)(writeto, buffer);
                            }
                        }
                        delete keys;
                    }
                }
            }

            RecordingSimpleCore::RecordingSimpleCore(const vector<uint32_t> &col_offset,
                                                     unique_ptr<AggReducer> reducer)
                    : SimpleCore(col_offset, move(reducer), false),
                      write_key_index_(col_offset.back() - 1) {}

            void RecordingSimpleCore::dump(MemTable &table, function<bool(DataRow &)> pred) {
                auto flex = table.allocateFlex();
                auto copier = reducer_->row_copier();
                auto field = static_cast<RecordingAggField *>(reducer_->fields()[0].get());
                if (!pred) {
                    auto keys = field->keys();
                    for (auto &key:*keys) {
                        DataRow &writeto = flex->push_back();
                        (*copier)(writeto, storage_);
                        writeto[write_key_index_] = key;
                    }
                    delete keys;
                } else {
                    MemDataRow buffer(table.colOffset());
                    auto keys = field->keys();
                    for (auto &key:*keys) {
                        (*copier)(buffer, storage_);
                        buffer[write_key_index_] = key;
                        if (pred(buffer)) {
                            DataRow &writeto = flex->push_back();
                            (*copier)(writeto, buffer);
                        }
                    }
                    delete keys;
                }
            }
        }
    }

    template<typename CORE>
    Agg<CORE>::Agg(function<bool(DataRow &)> pred, bool vertical)
            : Node(1), predicate_(pred), vertical_(vertical) {}

    template<typename CORE>
    unique_ptr<NodeOutput> Agg<CORE>::execute(const vector<NodeOutput *> &input) {
        auto input0 = static_cast<TableOutput *>(input[0]);
        auto result = agg(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(result));
    }

    using namespace std::placeholders;

    template<typename CORE>
    shared_ptr<Table> Agg<CORE>::agg(Table &input) {
        function<shared_ptr<CORE>(
                const shared_ptr<Block> &)> mapper = bind(&Agg::processBlock, this, _1);

        auto reducer = [](const shared_ptr<CORE> &a, const shared_ptr<CORE> &b) {
            a->merge(*b);
            return move(a);
        };
        auto merged = input.blocks()->map(mapper)->reduce(reducer);

        auto result = MemTable::Make(col_size_, vertical_);
        merged->dump(*result, predicate_);

        return result;
    }

    template<typename CORE>
    shared_ptr<CORE> Agg<CORE>::processBlock(const shared_ptr<Block> &block) {
        auto rows = block->rows();
        auto core = makeCore();
        uint64_t blockSize = block->size();

        for (uint32_t i = 0; i < blockSize; ++i) {
            core->reduce(rows->next());
        }
        return move(core);
    }

    // Subclass should override this
    template<typename CORE>
    shared_ptr<CORE> Agg<CORE>::makeCore() { return nullptr; }


    HashAgg::HashAgg(function<uint64_t(DataRow &)> hasher, unique_ptr<Snapshoter> header_copier,
                     function<vector<agg::AggField *>()> fields_gen,
                     function<bool(DataRow &)> pred, bool vertical)
            : Agg(pred, vertical), hasher_(hasher), header_copier_(move(header_copier)), fields_gen_(fields_gen) {
        auto &header_offset = header_copier_->colOffset();
        col_offset_.insert(col_offset_.end(), header_offset.begin(), header_offset.end());
        auto fields = fields_gen_();
        need_field_dump_ = false;
        for (auto field: fields) {
            fields_start_.push_back(col_offset_.back());
            col_offset_.push_back(col_offset_.back() + field->size());
            need_field_dump_ |= field->need_dump();
            delete field;
        }
        col_size_ = offset2size(col_offset_);
        row_copier_ = RowCopyFactory().buildAssign(I_RAW, I_RAW, col_offset_);
    }

    unique_ptr<AggReducer> HashAgg::createReducer() {
        return unique_ptr<AggReducer>(
                new AggReducer(header_copier_.get(), row_copier_.get(), fields_gen_(), fields_start_));
    }

    shared_ptr<HashCore> HashAgg::makeCore() {
        return make_shared<HashCore>(col_offset_, createReducer(), hasher_, need_field_dump_);
    }

    SimpleAgg::SimpleAgg(function<vector<agg::AggField *>()> fields_gen, function<bool(DataRow &)> pred,
                         bool vertical)
            : Agg(pred, vertical), fields_gen_(fields_gen) {
        col_offset_.push_back(0);
        auto fields = fields_gen_();
        need_field_dump_ = false;
        for (auto field: fields) {
            col_offset_.push_back(col_offset_.back() + field->size());
            need_field_dump_ |= field->need_dump();
            delete field;
        }
        col_size_ = offset2size(col_offset_);
        header_copier_ = RowCopyFactory().buildSnapshot();
        row_copier_ = RowCopyFactory().buildAssign(I_RAW, I_RAW, col_offset_);
    }

    unique_ptr<AggReducer> SimpleAgg::createReducer() {
        return unique_ptr<AggReducer>(
                new AggReducer(header_copier_.get(), row_copier_.get(), fields_gen_(), col_offset_));
    }

    shared_ptr<SimpleCore> SimpleAgg::makeCore() {
        return make_shared<SimpleCore>(col_offset_, createReducer(), need_field_dump_);
    }

    RecordingHashAgg::RecordingHashAgg(function<uint64_t(DataRow &)> hasher, unique_ptr<Snapshoter> header_copier,
                                       function<RecordingAggField *()> field_gen,
                                       function<bool(DataRow &)> pred, bool vertical)
            : Agg(pred, vertical), hasher_(hasher), header_copier_(move(header_copier)), field_gen_(field_gen) {
        auto &header_offset = header_copier_->colOffset();
        col_offset_.insert(col_offset_.end(), header_offset.begin(), header_offset.end());
        field_start_ = col_offset_.back();
        col_offset_.push_back(col_offset_.back() + 1);
        col_offset_.push_back(col_offset_.back() + 1);

        col_size_ = offset2size(col_offset_);
        row_copier_ = RowCopyFactory().buildAssign(I_RAW, I_RAW, col_offset_);
    }

    unique_ptr<AggReducer> RecordingHashAgg::createReducer() {
        return unique_ptr<AggReducer>(new AggReducer(
                header_copier_.get(), row_copier_.get(), field_gen_(), field_start_));
    }

    shared_ptr<RecordingHashCore> RecordingHashAgg::makeCore() {
        return make_shared<RecordingHashCore>(col_offset_, createReducer(), hasher_);
    }

    RecordingSimpleAgg::RecordingSimpleAgg(function<RecordingAggField *()> fields_gen,
                                           function<bool(DataRow &)> pred,
                                           bool vertical)
            : Agg(pred, vertical), field_gen_(fields_gen) {
        need_field_dump_ = false;
        col_offset_ = colOffset(2);
        header_copier_ = RowCopyFactory().buildSnapshot();
        row_copier_ = RowCopyFactory().buildAssign(I_RAW, I_RAW, col_offset_);
        col_size_ = colSize(2);
    }

    unique_ptr<AggReducer> RecordingSimpleAgg::createReducer() {
        return unique_ptr<AggReducer>(
                new AggReducer(header_copier_.get(), row_copier_.get(), field_gen_(), 0));
    }

    shared_ptr<RecordingSimpleCore> RecordingSimpleAgg::makeCore() {
        return make_shared<RecordingSimpleCore>(col_offset_, createReducer());
    }

}