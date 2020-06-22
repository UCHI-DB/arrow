//
// Created by Harper on 6/15/20.
//

#ifndef ARROW_DATA_CONTAINER_H
#define ARROW_DATA_CONTAINER_H

#include "lang.h"
#include "data_model.h"
#include "container.h"
#include "concurrent.h"

namespace lqf {
    namespace datacontainer {

        class MemDataRowAccessor : public DataRow {
        private:
            uint64_t *pointer_;
            vector<uint32_t> offset_;
            vector<uint32_t> size_;
            DataField view_;
        public:
            MemDataRowAccessor() = default;

            virtual ~MemDataRowAccessor() = default;

            void init(const vector<uint32_t> &offset);

            DataField &operator[](uint64_t i) override;

            DataRow &operator=(DataRow &row) override;

            unique_ptr<DataRow> snapshot() override;

            uint64_t *raw() override;

            inline uint32_t size() override { return offset_.back(); }

            inline uint32_t num_fields() override { return offset_.size() - 1; }

            inline void raw(uint64_t *p) { pointer_ = p; }

            inline const vector<uint32_t> &offset() const { return offset_; };

        };
// The buffer size actually affect running speed.
#define VECTOR_SLAB_SIZE_ 131072

        class MemRowVector {
        protected:
            MemDataRowAccessor accessor_;
            vector<shared_ptr<vector<uint64_t>>> memory_;
            uint32_t stripe_offset_;
            uint32_t size_;
            uint32_t row_size_ = 0;
            uint32_t stripe_size_ = 0;
        public:
            MemRowVector(const vector<uint32_t> &offset);

            DataRow &push_back();

            DataRow &operator[](uint32_t index);

            inline uint32_t size() { return size_; }

            inline vector<shared_ptr<vector<uint64_t>>> &memory() { return memory_; }

            unique_ptr<Iterator<DataRow &>> iterator();
        };

        class MemRowMap : public MemRowVector {
        protected:
            unordered_map<uint64_t, uint64_t> map_;
        public:
            MemRowMap(const vector<uint32_t> &offset);

            DataRow &insert(uint64_t key);

            DataRow &operator[](uint64_t key);

            DataRow *find(uint64_t);

            inline uint32_t size() { return map_.size(); }

            unique_ptr<Iterator<pair<uint64_t, DataRow &> &>> map_iterator();
        };

#define CMAP_SLAB_SIZE_ 131072

        struct MapAnchor {
            uint32_t index_;
            uint32_t offset_;
            MemDataRowAccessor accessor_;
        };

        class ConcurrentMemRowMap {
        protected:
            ThreadLocal<MapAnchor> anchor_;

            container::PhaseConcurrentIntHashMap map_;
            vector<shared_ptr<vector<uint64_t>>> memory_;
            vector<uint32_t> col_offset_;
            mutex memory_lock_;

            uint32_t row_size_;

            void new_slab();

            void init();

        public:
            ConcurrentMemRowMap(uint32_t expect_size, const vector<uint32_t> &);

            DataRow &insert(int key);

            DataRow &operator[](int key);

            DataRow *find(int key);

            uint32_t size();
        };

    }
}


#endif //ARROW_DATA_CONTAINER_H
