//
// Created by harper on 2/9/20.
//

#ifndef CHIDATA_LQF_DATA_MODEL_H
#define CHIDATA_LQF_DATA_MODEL_H

#include <cstdint>
#include <parquet/file_reader.h>
#include <chidata/stream.h>
#include <chidata/bitmap.h>
#include <parquet/column_page.h>

namespace chidata {
    namespace lqf {

        using namespace std;
        using namespace parquet;

        class DataField {
        private:
            uint64_t *value_;
        public:
            inline int32_t asInt() { return static_cast<int32_t>(*value_); }

            inline double asDouble() { return *((double *) value_); }

            inline void operator=(int32_t value) { *value_ = value; }

            inline void operator=(double value) { *value_ = *((uint64_t *) &value); }

            inline void operator=(DataField &df) { *value_ = *df.value_; }

            inline void operator=(uint64_t *value) { value_ = value; }

            inline uint64_t *data() { return value_; }

        };

        /*
         * A Union representing in-memory data fields
         */
        class DataRow {
        public:
            virtual DataField &operator[](uint64_t i) = 0;
        };

        class MemDataRow : public DataRow {
        private:
            vector<uint64_t> data_;
            DataField view_;
        public:
            MemDataRow(uint8_t num_fields) : data_(num_fields) {}

            virtual ~MemDataRow() {

            }

            virtual DataField &operator[](uint64_t i) override {
                view_ = data_.data() + i;
                return view_;
            }

            inline void operator=(MemDataRow &row) {
                memcpy(static_cast<void *>(data_.data()), static_cast<void *>(row.data_.data()),
                       sizeof(uint64_t) * data_.size());
            }

            inline uint32_t size() {
                return data_.size();
            }
        };

        class DataRowIterator {
        public:
            virtual DataRow &operator[](uint64_t idx) = 0;

            virtual DataRow &next() = 0;

            virtual uint64_t pos() = 0;
        };

        class ColumnIterator {
        public:
            virtual DataField &next() = 0;

            virtual DataField &operator[](uint64_t idx) = 0;

            virtual uint64_t pos() = 0;
        };

        class Block : public enable_shared_from_this<Block> {
        public:

            virtual uint64_t size() = 0;

            virtual unique_ptr<ColumnIterator> col(uint32_t col_index) = 0;

            virtual unique_ptr<DataRowIterator> rows() = 0;

            virtual shared_ptr<Block> mask(shared_ptr<Bitmap> mask) = 0;
        };

        class MemBlock : public Block {
        private:
            uint32_t size_;
            uint8_t num_fields_;
            vector<uint64_t> content_;
        public:
            MemBlock(uint32_t size, uint8_t num_fields);

            virtual ~MemBlock();

            uint64_t size() override;

            /**
             * Increase the block row
             * @param size
             */
            void inc(uint32_t row_to_inc);

            void compact(uint32_t newsize);

            unique_ptr<ColumnIterator> col(uint32_t col_index) override;

            unique_ptr<DataRowIterator> rows() override;

            shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
        };

        template<typename DTYPE>
        class Dictionary {
        private:
            using T = typename DTYPE::c_type;
            vector<T> buffer_;
        public:
            Dictionary(DictionaryPage *data);

            uint32_t lookup(T key);
        };

        using Int32Dictionary = Dictionary<Int32Type>;
        using DoubleDictionary = Dictionary<DoubleType>;

        template<typename DTYPE>
        class RawAccessor {
        protected:
            unique_ptr<Dictionary<DTYPE>> dict_;

            uint8_t *data(DataPage *);

        public:
            virtual void dict(parquet::DictionaryPage *dictPage) {
                dict_ = unique_ptr<Dictionary<DTYPE>>(new Dictionary<DTYPE>(dictPage));
            }

            virtual void filter(parquet::DataPage *, shared_ptr<Bitmap>) = 0;
        };

        using Int32Accessor = RawAccessor<Int32Type>;
        using DoubleAccessor = RawAccessor<DoubleType>;

        class ParquetBlock : public Block {
        private:
            shared_ptr<RowGroupReader> rowGroup_;
            uint32_t index_;
            uint64_t columns_;
        public:
            ParquetBlock(shared_ptr<RowGroupReader>, uint32_t, uint64_t);

            virtual ~ParquetBlock();

            uint64_t size() override;

            template<typename DTYPE>
            shared_ptr<Bitmap> raw(uint32_t col_index, RawAccessor<DTYPE> *);

            unique_ptr<ColumnIterator> col(uint32_t col_index) override;

            unique_ptr<DataRowIterator> rows() override;

            shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
        };

        class MaskedBlock : public Block {
            shared_ptr<ParquetBlock> inner_;
            shared_ptr<Bitmap> mask_;
        public:
            MaskedBlock(shared_ptr<ParquetBlock> inner, shared_ptr<Bitmap> mask);

            virtual ~MaskedBlock();

            uint64_t size() override;

            unique_ptr<ColumnIterator> col(uint32_t col_index) override;

            unique_ptr<DataRowIterator> rows() override;

            shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
        };

        class Table {
        public:
            virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() = 0;
        };

        class ParquetTable : public Table {
        private:
            uint64_t columns_;
        public:
            ParquetTable(const string &fileName);

            virtual ~ParquetTable();

            virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

            void updateColumns(uint64_t columns);

            static shared_ptr<ParquetTable> Open(const string &filename);

        protected:

            shared_ptr<ParquetBlock> createParquetBlock(const int &block_idx);

        private:
            unique_ptr<ParquetFileReader> fileReader_;
        };

        class TableView : public Table {
            shared_ptr<Stream<shared_ptr<Block>>> stream_;
        public:
            TableView(shared_ptr<Stream<shared_ptr<Block>>>);

            shared_ptr<Stream<shared_ptr<Block> > > blocks() override;
        };

        class MemTable : public Table {
        private:
            uint8_t num_fields_;
            vector<shared_ptr<Block>> blocks_;
        protected:
            MemTable(uint8_t num_fields);

        public:
            static shared_ptr<MemTable> Make(uint8_t num_fields);

            virtual ~MemTable();

            shared_ptr<MemBlock> allocate(uint32_t num_rows);

            virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() override;
        };

    }
}

#endif //CHIDATA_LQF_DATA_MODEL_H