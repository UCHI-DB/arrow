//
// Created by Harper on 6/15/20.
//

#include "data_container.h"

using namespace lqf;

namespace lqf {
    namespace datacontainer {

        void MemDataRowAccessor::init(const vector<uint32_t> &offset) {
            offset_ = offset;
            size_ = offset2size(offset);
        }

        DataField &MemDataRowAccessor::operator[](uint64_t i) {
            view_ = pointer_ + offset_[i];
            view_.size_ = size_[i];
            return view_;
        }

        uint64_t *MemDataRowAccessor::raw() {
            return pointer_;
        }

        unique_ptr<DataRow> MemDataRowAccessor::snapshot() {
            return nullptr;
        }

        DataRow &MemDataRowAccessor::operator=(DataRow &row) {
            if (row.raw()) {
                memcpy(static_cast<void *>(pointer_), static_cast<void *>(row.raw()),
                       sizeof(uint64_t) * offset_.back());
            } else {
                auto offset_size = offset_.size();
                for (uint32_t i = 0; i < offset_size - 1; ++i) {
                    (*this)[i] = row[i];
                }
            }
            return *this;
        }

        MemRowVector::MemRowVector(const vector<uint32_t> &offset)
                : stripe_offset_(0), size_(0) {
            accessor_.init(offset);
            memory_.push_back(make_shared<vector<uint64_t>>(VECTOR_SLAB_SIZE_));
            row_size_ = offset.back();
            stripe_size_ = VECTOR_SLAB_SIZE_ / row_size_;
        }

        DataRow &MemRowVector::push_back() {
            auto current = stripe_offset_;
            stripe_offset_ += row_size_;
            if (stripe_offset_ > VECTOR_SLAB_SIZE_) {
                memory_.push_back(make_shared<vector<uint64_t>>(VECTOR_SLAB_SIZE_));
                stripe_offset_ = row_size_;
                current = 0;
            }
            size_++;
            accessor_.raw(memory_.back()->data() + current);
            return accessor_;
        }

        DataRow &MemRowVector::operator[](uint32_t index) {
            auto stripe_index = index / stripe_size_;
            auto offset = index % stripe_size_;
            accessor_.raw(memory_[stripe_index]->data() + offset * row_size_);
            return accessor_;
        }

        class MemRowVectorIterator : public Iterator<DataRow &> {
        protected:
            vector<shared_ptr<vector<uint64_t>>> &memory_ref_;
            uint32_t index_;
            uint32_t pointer_;
            uint32_t counter_;
            uint32_t size_;
            MemDataRowAccessor accessor_;
            uint32_t row_size_;
        public:
            MemRowVectorIterator(vector<shared_ptr<vector<uint64_t>>> &ref,
                                 const vector<uint32_t> &offset, uint32_t size)
                    : memory_ref_(ref), index_(0), pointer_(0), counter_(0),
                      size_(size), row_size_(offset.back()) {
                accessor_.init(offset);
            }

            bool hasNext() override {
                return counter_ < size_;
            }

            DataRow &next() override {
                accessor_.raw(memory_ref_[index_]->data() + pointer_);
                pointer_ += row_size_;
                if (pointer_ > VECTOR_SLAB_SIZE_) {
                    ++index_;
                    pointer_ = 0;
                    accessor_.raw(memory_ref_[index_]->data() + pointer_);
                    pointer_ += row_size_;
                }
                ++counter_;
                return accessor_;
            }
        };

        unique_ptr<Iterator<DataRow &>> MemRowVector::iterator() {
            return unique_ptr<Iterator<DataRow &>>(new MemRowVectorIterator(memory_, accessor_.offset(), size_));
        }

        MemRowMap::MemRowMap(const vector<uint32_t> &offset) : MemRowVector(offset) {}

        DataRow &MemRowMap::insert(uint64_t key) {
            push_back();
            uint64_t anchor = (static_cast<uint64_t>(memory_.size() - 1) << 32) | (stripe_offset_ - row_size_);
            map_[key] = anchor;
            return accessor_;
        }

        DataRow &MemRowMap::operator[](uint64_t key) {
            auto found = map_.find(key);
            if (found == map_.end()) {
                return insert(key);
            }
            auto anchor = found->second;
            auto index = static_cast<uint32_t>(anchor >> 32);
            auto offset = static_cast<uint32_t>(anchor);
            accessor_.raw(memory_[index]->data() + offset);
            return accessor_;
        }

        DataRow *MemRowMap::find(uint64_t key) {
            auto found = map_.find(key);
            if (found == map_.end()) {
                return nullptr;
            }
            auto anchor = found->second;
            auto index = static_cast<uint32_t>(anchor >> 32);
            auto offset = static_cast<uint32_t>(anchor);
            accessor_.raw(memory_[index]->data() + offset);
            return &accessor_;
        }

        class MemRowMapIterator : public Iterator<pair<uint64_t, DataRow &> &> {
        protected:
            vector<shared_ptr<vector<uint64_t>>> &memory_ref_;
            unordered_map<uint64_t, uint64_t>::iterator map_it_;
            unordered_map<uint64_t, uint64_t>::const_iterator map_end_;
            MemDataRowAccessor accessor_;
            pair<uint64_t, DataRow &> pair_;
        public:
            MemRowMapIterator(vector<shared_ptr<vector<uint64_t>>> &ref,
                              const vector<uint32_t> &offset, unordered_map<uint64_t, uint64_t> &map)
                    : memory_ref_(ref), map_it_(map.begin()), map_end_(map.cend()), pair_(0, accessor_) {
                accessor_.init(offset);
            }

            bool hasNext() override {
                return map_it_ != map_end_;
            }

            pair<uint64_t, DataRow &> &next() override {
                pair_.first = map_it_->first;
                auto anchor = map_it_->second;
                auto index = static_cast<uint32_t>(anchor >> 32);
                auto pointer = static_cast<uint32_t>(anchor);
                accessor_.raw(memory_ref_[index]->data() + pointer);
                map_it_++;
                return pair_;
            }
        };

        unique_ptr<Iterator<pair<uint64_t, DataRow &> &>> MemRowMap::map_iterator() {
            return unique_ptr<Iterator<pair<uint64_t, DataRow &> &>>(
                    new MemRowMapIterator(memory_, accessor_.offset(), map_));
        }

        ConcurrentMemRowMap::ConcurrentMemRowMap(uint32_t expect_size, const vector<uint32_t> &col_offset)
                : anchor_([col_offset]() {
            MapAnchor anchor;
            anchor.offset_ = CMAP_SLAB_SIZE_;
            anchor.accessor_.init(col_offset);
            return anchor;
        }), map_(expect_size), memory_(1000), col_offset_(col_offset), row_size_(col_offset.back()) {}

        void ConcurrentMemRowMap::new_slab() {
            auto anchor = anchor_.get();
            memory_lock_.lock();
            anchor->index_ = memory_.size();
            memory_.push_back(make_shared<vector<uint64_t>>(CMAP_SLAB_SIZE_));
            anchor->offset_ = 0;
            memory_lock_.unlock();
        }

        DataRow &ConcurrentMemRowMap::insert(int key) {
            auto anchor = anchor_.get();
            if (anchor->offset_ + row_size_ > CMAP_SLAB_SIZE_) {
                new_slab();
            }
            anchor->accessor_.raw(memory_[anchor->index_]->data() + anchor->offset_);
            map_.put(key, (anchor->index_ << 20) | anchor->offset_);
            anchor->offset_ += row_size_;
            return anchor->accessor_;
        }

        DataRow &ConcurrentMemRowMap::operator[](int key) {
            auto anchor = anchor_.get();
            int pos = map_.get(key);
            if (pos == INT32_MIN) {
                return insert(key);
            }
            int index = pos >> 20;
            int offset = pos & 0xFFFFF;
            anchor->accessor_.raw(memory_[index]->data() + offset);
            return anchor->accessor_;
        }

        DataRow *ConcurrentMemRowMap::find(int key) {
            auto anchor = anchor_.get();
            int pos = map_.get(key);
            if (pos == INT32_MIN) {
                return nullptr;
            }
            int index = pos >> 20;
            int offset = pos & 0xFFFFF;
            anchor->accessor_.raw(memory_[index]->data() + offset);
            return &(anchor->accessor_);
        }

        uint32_t ConcurrentMemRowMap::size() {
            return map_.size();
        }
    }
}