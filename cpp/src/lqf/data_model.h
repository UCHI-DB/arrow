//
// Created by harper on 2/9/20.
//

#ifndef CHIDATA_LQF_DATA_MODEL_H
#define CHIDATA_LQF_DATA_MODEL_H

#include <cstdint>
#include <iostream>
#include <arrow/util/bit_stream_utils.h>
#include <parquet/file_reader.h>
#include <parquet/column_page.h>
#include "stream.h"
#include "bitmap.h"

namespace lqf {

    using namespace std;
    using namespace parquet;

    union DataField {
        uint64_t *raw_;
        int32_t *ival_;
        double *dval_;
        ByteArray *sval_;
    public:
        inline int32_t asInt() const { return *ival_; }

        inline double asDouble() const { return *dval_; }

        inline ByteArray *asByteArray() const { return sval_; }

        inline void operator=(int32_t value) { *ival_ = value; }

        inline void operator=(double value) { *dval_ = value; }

        inline void operator=(ByteArray *value) { sval_ = value; }

        inline void operator=(uint64_t *raw) { raw_ = raw; };

        inline uint64_t *data() const { return raw_; }

        inline void operator=(DataField &df) { *raw_ = *df.raw_; }
    };

    /*
     * A Union representing in-memory data fields
     */
    class DataRow {
    public:
        virtual ~DataRow();

        virtual DataField &operator[](uint64_t i) = 0;

        virtual DataField &operator()(uint64_t i) {
            return (*this)[i];
        }

        virtual void operator=(DataRow &row) {}

        virtual uint64_t *raw() {
            return nullptr;
        }
    };

    class MemDataRow : public DataRow {
    private:
        vector<uint64_t> data_;
        DataField view_;
    public:
        MemDataRow(uint8_t num_fields);

        virtual ~MemDataRow();

        DataField &operator[](uint64_t i) override;

        void operator=(DataRow &row) override;

        uint64_t *raw() override;

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

        virtual DataField &operator()(uint64_t idx) {
            return (*this)[idx];
        }

        virtual uint64_t pos() = 0;
    };

    class Table;

    class Block : public enable_shared_from_this<Block> {
    public:
        virtual Table *owner() { return nullptr; }

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

        inline vector<uint64_t> &content();

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

    };

    template<typename DTYPE>
    class Dictionary {
    private:
        using T = typename DTYPE::c_type;
        // This page need to be cached. Otherwise when it is released, byte array data may be lost.
        shared_ptr<DictionaryPage> page_;
        T *buffer_;
        uint32_t size_;
    public:
        Dictionary();

        Dictionary(shared_ptr<DictionaryPage> data);

        virtual ~Dictionary();

        int32_t lookup(const T &key);

        unique_ptr<vector<uint32_t>> list(function<bool(const T &)>);

        Dictionary<DTYPE> &operator=(Dictionary<DTYPE> &&other) {
            this->buffer_ = other.buffer_;
            other.buffer_ = nullptr;
            this->size_ = other.size_;
            return *this;
        }

        inline uint32_t size() {
            return size_;
        }
    };

    using Int32Dictionary = Dictionary<Int32Type>;
    using DoubleDictionary = Dictionary<DoubleType>;
    using ByteArrayDictionary = Dictionary<ByteArrayType>;

    template<typename DTYPE>
    class RawAccessor {
    protected:

        shared_ptr<SimpleBitmap> bitmap_;

        uint64_t offset_;

        virtual void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) {};

    public:
        RawAccessor() : offset_(0) {}

        virtual ~RawAccessor() {}

        virtual void init(uint64_t size) {
            bitmap_ = make_shared<SimpleBitmap>(size);
            offset_ = 0;
        }

        virtual void dict(Dictionary<DTYPE> &dict) {}

        virtual void data(DataPage *dpage) {
            // Assume all fields are mandatory, which is true for TPCH
            scanPage(dpage->num_values(), dpage->data(), bitmap_->raw(), offset_);
            offset_ += dpage->num_values();
        }

        inline shared_ptr<Bitmap> result() {
            return bitmap_;
        }
    };

    using Int32Accessor = RawAccessor<Int32Type>;
    using DoubleAccessor = RawAccessor<DoubleType>;
    using ByteArrayAccessor = RawAccessor<ByteArrayType>;

    class ParquetTable;

    class ParquetBlock : public Block {
    private:
        ParquetTable *owner_;
        shared_ptr<RowGroupReader> rowGroup_;
        uint32_t index_;
        uint64_t columns_;
    public:
        ParquetBlock(ParquetTable *, shared_ptr<RowGroupReader>, uint32_t, uint64_t);

        virtual ~ParquetBlock();

        uint64_t size() override;

        inline uint32_t index() { return index_; }

        Table *owner() override;

        template<typename DTYPE>
        shared_ptr<Bitmap> raw(uint32_t col_index, RawAccessor<DTYPE> *accessor);

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        unique_ptr<ParquetFileReader> fileReader_;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
    };

    class MaskedBlock : public Block {
        shared_ptr<ParquetBlock> inner_;
        shared_ptr<Bitmap> mask_;
    public:
        MaskedBlock(shared_ptr<ParquetBlock> inner, shared_ptr<Bitmap> mask);

        virtual ~MaskedBlock();

        uint64_t size() override;

        inline shared_ptr<ParquetBlock> inner() { return inner_; }

        inline shared_ptr<Bitmap> mask() { return mask_; }

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
    };

    class Table {
    public:
        virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() = 0;

        /**
         * The number of columns in the table
         * @return
         */
        virtual uint32_t numFields() = 0;

        uint64_t size();
    };

    class ParquetTable : public Table {
    private:
        uint64_t columns_;

        unique_ptr<ParquetFileReader> fileReader_;
    public:
        ParquetTable(const string &fileName, uint64_t columns = 0);

        virtual ~ParquetTable();

        virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint32_t numFields() override;

        void updateColumns(uint64_t columns);

        inline uint64_t size() { return fileReader_->metadata()->num_rows(); }

        static shared_ptr<ParquetTable> Open(const string &filename, uint64_t columns = 0);

        static shared_ptr<ParquetTable> Open(const string &filename, std::initializer_list<uint32_t> columns);

    protected:
        shared_ptr<ParquetBlock> createParquetBlock(const int &block_idx);

    };

    class MaskedTable : public Table {
    private:
        ParquetTable *inner_;
        vector<shared_ptr<Bitmap>> masks_;
    public:

        MaskedTable(ParquetTable *, unordered_map<uint32_t, shared_ptr<Bitmap>> &);

        virtual ~MaskedTable();

        virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint32_t numFields() override;

    protected:
        shared_ptr<Block> buildMaskedBlock(const shared_ptr<Block> &);
    };

    class TableView : public Table {
        uint32_t num_fields_;
        shared_ptr<Stream<shared_ptr<Block>>> stream_;
    public:
        TableView(uint32_t, shared_ptr<Stream<shared_ptr<Block>>>);

        shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint32_t numFields() override;
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

        virtual shared_ptr<MemBlock> allocate(uint32_t num_rows);

        shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint32_t numFields() override;
    };

    class ParallelMemTable : MemTable {
    protected:
        mutex lock_;
    public:
        shared_ptr<MemBlock> allocate(uint32_t) override;
    };

}

#endif //CHIDATA_LQF_DATA_MODEL_H
