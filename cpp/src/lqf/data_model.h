//
// Created by harper on 2/9/20.
//

#ifndef CHIDATA_LQF_DATA_MODEL_H
#define CHIDATA_LQF_DATA_MODEL_H

#include <cstdint>
#include <random>
#include <iostream>
#include <arrow/util/bit_stream_utils.h>
#include <parquet/file_reader.h>
#include <parquet/column_page.h>
#include "stream.h"
#include "bitmap.h"

namespace lqf {

    using namespace std;
    using namespace parquet;

    const vector<uint32_t> &colOffset(uint32_t num_fields);

    const vector<uint32_t> &colSize(uint32_t num_fields);

    union DataPointer {
        uint64_t *raw_;
        int32_t *ival_;
        double *dval_;
        ByteArray *sval_;
    };

    class DataField {
    public:
        DataPointer pointer_;
        uint8_t size_;

        inline int32_t asInt() const { return *pointer_.ival_; }

        inline double asDouble() const { return *pointer_.dval_; }

        inline ByteArray &asByteArray() const { return *pointer_.sval_; }

        inline void operator=(int32_t value) { *pointer_.ival_ = value; }

        inline void operator=(double value) { *pointer_.dval_ = value; }

        inline void operator=(ByteArray &value) { *pointer_.sval_ = value; }

        inline void operator=(uint64_t *raw) { pointer_.raw_ = raw; };

        inline uint64_t *data() const { return pointer_.raw_; }

        inline void operator=(DataField &df) {
            assert(size_ <= 2);
            assert(df.size_ <= 2);
            assert(df.size_ <= size_);
            // We allow the input size to be smaller than local size, allowing a larger field
            // to be used for smaller field
            memcpy((void *) pointer_.raw_, (void *) df.pointer_.raw_, sizeof(uint64_t) * df.size_);
//            *raw_ = *df.raw_;
        }
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
        const vector<uint32_t> &offset_;
        DataField view_;
    public:
        MemDataRow(uint8_t num_fields);

        MemDataRow(const vector<uint32_t> &offset);

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

        virtual void translate(DataField &, uint32_t, uint32_t) = 0;
    };

    class ColumnIterator {
    public:
        virtual DataField &next() = 0;

        virtual DataField &operator[](uint64_t idx) = 0;

        virtual DataField &operator()(uint64_t idx) {
            return (*this)[idx];
        }

        virtual void translate(DataField &, uint32_t) = 0;

        virtual uint64_t pos() = 0;
    };

    class Table;

    class Block : public enable_shared_from_this<Block> {
    private:
        static mt19937 rand_;
    protected:
        uint32_t id_;
    public:
        Block(uint32_t id) : id_(id) {}

        Block() : Block(rand_()) {}

        virtual Table *owner() { return nullptr; }

        inline uint32_t id() { return id_; };

        /// Number of rows in the block
        virtual uint64_t size() = 0;

        /// Block limit, used for creating bitmaps
        virtual uint64_t limit() { return size(); };

        virtual unique_ptr<ColumnIterator> col(uint32_t col_index) = 0;

        virtual unique_ptr<DataRowIterator> rows() = 0;

        virtual shared_ptr<Block> mask(shared_ptr<Bitmap> mask) = 0;

        virtual void compact(uint32_t newsize) {}
    };

    class MemBlock : public Block {
    private:
        uint32_t size_;
        uint32_t row_size_;
        const vector<uint32_t> &col_offset_;

        vector<uint64_t> content_;
    public:
        MemBlock(uint32_t size, uint32_t row_size, const vector<uint32_t> &col_offset);

        MemBlock(uint32_t size, uint32_t row_size);

        virtual ~MemBlock();

        uint64_t size() override;

        /**
         * Increase the block row
         * @param size
         */
        void inc(uint32_t row_to_inc);

        void compact(uint32_t newsize) override;

        inline vector<uint64_t> &content();

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

    };

    class MemvBlock : public Block {
    private:
        uint32_t size_;
        const vector<uint32_t> &col_size_;

        vector<unique_ptr<vector<uint64_t>>> content_;
    public:
        MemvBlock(uint32_t size, const vector<uint32_t> &col_size);

        MemvBlock(uint32_t size, uint32_t num_fields);

        virtual ~MemvBlock();

        uint64_t size() override;

        void inc(uint32_t row_to_inc);

        void compact(uint32_t newsize) override;

        inline vector<uint64_t> &content();

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

        void merge(MemvBlock &, const vector<pair<uint8_t, uint8_t>> &);
    };

    template<typename DTYPE>
    class Dictionary {
    private:
        using T = typename DTYPE::c_type;
        // This page need to be cached. Otherwise when it is released, byte array data may be lost.
        bool managed_ = true;
        shared_ptr<DictionaryPage> page_;
        T *buffer_;
        uint32_t size_;
    public:
        Dictionary();

        Dictionary(shared_ptr<DictionaryPage> data);

        Dictionary(T *buffer, uint32_t size);

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
    protected:
        ParquetTable *owner_;
        shared_ptr<RowGroupReader> rowGroup_;
        uint32_t index_;
        uint64_t columns_;

        vector<void *> dictionaries_;
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

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

    };

    class MaskedBlock : public Block {
        shared_ptr<Block> inner_;
        shared_ptr<Bitmap> mask_;
    public:
        MaskedBlock(shared_ptr<Block> inner, shared_ptr<Bitmap> mask);

        virtual ~MaskedBlock();

        uint64_t size() override;

        uint64_t limit() override;

        inline shared_ptr<Block> inner() { return inner_; }

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
        virtual uint8_t numFields() = 0;

        virtual uint8_t numStringFields() { return 0; }

        uint64_t size();
    };

    class ParquetTable : public Table {
    private:
        const string name_;

        uint64_t columns_;

        unique_ptr<ParquetFileReader> fileReader_;
    public:
        ParquetTable(const string &fileName, uint64_t columns = 0);

        virtual ~ParquetTable();

        virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint8_t numFields() override;

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

        uint8_t numFields() override;

    protected:
        shared_ptr<Block> buildMaskedBlock(const shared_ptr<Block> &);
    };

    class TableView : public Table {
        uint32_t num_fields_;
        shared_ptr<Stream<shared_ptr<Block>>> stream_;
    public:
        TableView(uint32_t, shared_ptr<Stream<shared_ptr<Block>>>);

        shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint8_t numFields() override;
    };

    class MemTable : public Table {
    private:;
        bool vertical_;

        // For columnar view
        vector<uint32_t> col_size_;

        // For row view
        uint32_t row_size_;
        vector<uint32_t> col_offset_;

        vector<shared_ptr<Block>> blocks_;
    protected:
        MemTable(const vector<uint32_t> col_size, bool vertical);

    public:
        static shared_ptr<MemTable> Make(uint8_t num_fields, bool vertical = false);

        static shared_ptr<MemTable> Make(uint8_t num_fields, uint8_t num_string_fields, bool vertical = false);

        static shared_ptr<MemTable> Make(const vector<uint32_t> col_size, bool vertical);

        virtual ~MemTable();

        shared_ptr<Block> allocate(uint32_t num_rows);

        void append(shared_ptr<Block>);

        shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        uint8_t numFields() override;

        uint8_t numStringFields() override;

        const vector<uint32_t> &colSize();

        const vector<uint32_t> &colOffset();
    };

}

#endif //CHIDATA_LQF_DATA_MODEL_H
