//
// Created by harper on 2/9/20.
//

#include <iostream>
#include <exception>
#include <arrow/util/bit_stream_utils.h>
#include <parquet/encoding.h>
#include <parquet/column_reader.h>
#include "validate.h"
#include "data_model.h"

using namespace std;

namespace lqf {

    DataRow::~DataRow() {

    }

    MemDataRow::MemDataRow(uint8_t num_fields) : data_(num_fields, 0x0) {}

    MemDataRow::~MemDataRow() {}

    DataField &MemDataRow::operator[](uint64_t i) {
        view_ = data_.data() + i;
        return view_;
    }

    void MemDataRow::operator=(DataRow &row) {
        memcpy(static_cast<void *>(data_.data()), static_cast<void *>(row.raw()),
               sizeof(uint64_t) * data_.size());
    }

    uint64_t *MemDataRow::raw() {
        return data_.data();
    }

    MemBlock::MemBlock(uint32_t size, uint8_t num_fields) : size_(size), num_fields_(num_fields) {
        content_ = vector<uint64_t>(size * num_fields);
    }

    MemBlock::~MemBlock() {
    }

    uint64_t MemBlock::size() {
        return size_;
    }

    void MemBlock::inc(uint32_t row_to_inc) {
        content_.resize(content_.size() + num_fields_ * row_to_inc);
        size_ += row_to_inc;
    }

    void MemBlock::compact(uint32_t newsize) {
        content_.resize(newsize * num_fields_);
        size_ = newsize;
    }

    vector<uint64_t> &MemBlock::content() {
        return content_;
    }

    class MemDataRowIterator;

    class MemDataRowView : public DataRow {
    private:
        vector<uint64_t> &data_;
        uint64_t index_;
        uint8_t num_fields_;
        DataField view_;
        friend MemDataRowIterator;
    public:
        MemDataRowView(vector<uint64_t> &data, uint8_t num_fields)
                : data_(data), index_(-1), num_fields_(num_fields) {}

        virtual ~MemDataRowView() {

        }

        void moveto(uint64_t index) {
            index_ = index;
        }

        void next() {
            ++index_;
        }

        DataField &operator[](uint64_t i) override {
            view_ = data_.data() + index_ * num_fields_ + i;
            return view_;
        }

        uint64_t *raw() override {
            return data_.data() + index_ * num_fields_;
        }

        void operator=(DataRow &row) override {
            memcpy(static_cast<void *>(data_.data() + index_ * num_fields_), static_cast<void *>(row.raw()),
                   sizeof(uint64_t) * num_fields_);
        }

    };

    class MemDataRowIterator : public DataRowIterator {
    private:
        MemDataRowView reference_;
    public:
        MemDataRowIterator(vector<uint64_t> &data, uint8_t num_fields)
                : reference_(data, num_fields) {}

        virtual DataRow &operator[](uint64_t idx) override {
            reference_.moveto(idx);
            return reference_;
        }

        virtual DataRow &next() override {
            reference_.next();
            return reference_;
        }

        virtual uint64_t pos() override {
            return reference_.index_;
        }
    };

    class MemColumnIterator : public ColumnIterator {
    private:
        uint8_t num_fields_;
        vector<uint64_t> &data_;
        uint8_t colindex_;
        uint64_t rowindex_;
        DataField view_;
    public:
        MemColumnIterator(vector<uint64_t> &data, uint8_t num_fields, uint8_t colindex)
                : num_fields_(num_fields), data_(data), colindex_(colindex), rowindex_(-1) {}

        virtual DataField &operator[](uint64_t idx) override {
            rowindex_ = idx;
            view_ = data_.data() + idx * num_fields_ + colindex_;
            return view_;
        }

        DataField &next() override {
            ++rowindex_;
            view_ = data_.data() + rowindex_ * num_fields_ + colindex_;
            return view_;
        }

        uint64_t pos() override {
            return rowindex_;
        }

    };

    unique_ptr<DataRowIterator> MemBlock::rows() {
        return unique_ptr<DataRowIterator>(new MemDataRowIterator(content_, num_fields_));
    }

    unique_ptr<ColumnIterator> MemBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new MemColumnIterator(content_, num_fields_, col_index));
    }

    shared_ptr<Block> MemBlock::mask(shared_ptr<Bitmap> mask) {
        auto newBlock = make_shared<MemBlock>(mask->cardinality(), num_fields_);
        auto ite = mask->iterator();

        auto newData = newBlock->content_.data();
        auto oldData = content_.data();

        auto newCounter = 0;
        while (ite->hasNext()) {
            auto next = ite->next();
            memcpy((void *) (newData + (newCounter++) * num_fields_),
                   (void *) (oldData + next * num_fields_),
                   sizeof(uint64_t) * num_fields_);
        }
        return newBlock;
    }

    MaskedBlock::MaskedBlock(shared_ptr<ParquetBlock> inner, shared_ptr<Bitmap> mask)
            : inner_(inner), mask_(mask) {}

    MaskedBlock::~MaskedBlock() {}

    uint64_t MaskedBlock::size() {
        return mask_->cardinality();
    }

    class MaskedColumnIterator : public ColumnIterator {
    private:
        unique_ptr<ColumnIterator> inner_;
        unique_ptr<BitmapIterator> bite_;
    public:
        MaskedColumnIterator(unique_ptr<ColumnIterator> inner, unique_ptr<BitmapIterator> bite)
                : inner_(move(inner)), bite_(move(bite)) {}

        virtual DataField &operator[](uint64_t index) override {
            return (*inner_)[index];
        }

        virtual DataField &next() override {
            return (*inner_)[bite_->next()];
        }

        uint64_t pos() override {
            return inner_->pos();
        }
    };

    unique_ptr<ColumnIterator> MaskedBlock::col(uint32_t col_index) {
        return unique_ptr<MaskedColumnIterator>(new MaskedColumnIterator(inner_->col(col_index),
                                                                         mask_->iterator()));
    }

    class MaskedRowIterator : public DataRowIterator {
    private:
        unique_ptr<DataRowIterator> inner_;
        unique_ptr<BitmapIterator> bite_;
    public:
        MaskedRowIterator(unique_ptr<DataRowIterator> inner, unique_ptr<BitmapIterator> bite)
                : inner_(move(inner)), bite_(move(bite)) {}

        virtual DataRow &operator[](uint64_t index) override {
            return (*inner_)[index];
        }

        virtual DataRow &next() override {
            return (*inner_)[bite_->next()];
        }

        uint64_t pos() override {
            return inner_->pos();
        }
    };

    unique_ptr<DataRowIterator> MaskedBlock::rows() {
        return unique_ptr<MaskedRowIterator>(new MaskedRowIterator(inner_->rows(), mask_->iterator()));
    }

    shared_ptr<Block> MaskedBlock::mask(shared_ptr<Bitmap> mask) {
        this->mask_ = (*this->mask_) & *mask;
        return this->shared_from_this();
    }

    using namespace parquet;

    template<typename DTYPE>
    Dictionary<DTYPE>::Dictionary(shared_ptr<DictionaryPage> dpage) {
        this->page_ = dpage;
        auto decoder = parquet::MakeTypedDecoder<DTYPE>(Encoding::PLAIN, nullptr);
        size_ = dpage->num_values();
        decoder->SetData(size_, dpage->data(), dpage->size());
        buffer_ = (T *) malloc(sizeof(T) * size_);
        decoder->Decode(buffer_, size_);
    }

    template<typename DTYPE>
    Dictionary<DTYPE>::~Dictionary() {
        free(buffer_);
    }

    template<typename DTYPE>
    int32_t Dictionary<DTYPE>::lookup(const T &key) {
        uint32_t low = 0;
        uint32_t high = size_;

        while (low <= high) {
            uint32_t mid = (low + high) >> 1;
            T midVal = buffer_[mid];

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    ParquetBlock::ParquetBlock(shared_ptr<RowGroupReader> rowGroup, uint32_t index, uint64_t columns)
            : rowGroup_(rowGroup), index_(index), columns_(columns) {}

    ParquetBlock::~ParquetBlock() {

    }

    uint64_t ParquetBlock::size() {
        return rowGroup_->metadata()->num_rows();
    }

    class ParquetRowIterator;

    class ParquetRowView : public DataRow {
    protected:
        vector<unique_ptr<ColumnIterator>> &columns_;
        uint64_t index_;
        friend ParquetRowIterator;
    public:
        ParquetRowView(vector<unique_ptr<ColumnIterator>> &cols) : columns_(cols), index_(-1) {}

        virtual DataField &operator[](uint64_t colindex) override {
//            validate_true(colindex < columns_.size() && columns_[colindex], "column not available");
            return (*(columns_[colindex]))[index_];
        }

        virtual DataField &operator()(uint64_t colindex) override {
//            validate_true(colindex < columns_.size() && columns_[colindex], "column not available");
            return (*(columns_[colindex]))(index_);
        }

        void setIndex(uint64_t index) {
            this->index_ = index;
        }
    };

    template<typename DTYPE>
    shared_ptr<Bitmap> ParquetBlock::raw(uint32_t col_index, RawAccessor<DTYPE> *accessor) {
        accessor->init(this->size());
        auto pageReader = rowGroup_->GetColumnPageReader(col_index);
        shared_ptr<Page> page = pageReader->NextPage();

        if (page->type() == PageType::DICTIONARY_PAGE) {
            accessor->dict(static_pointer_cast<DictionaryPage>(page));
        } else {
            accessor->data((DataPage *) page.get());
        }
        while ((page = pageReader->NextPage())) {
            accessor->data((DataPage *) page.get());
        }
        return accessor->result();
    }

    class ParquetColumnIterator;

    class ParquetRowIterator : public DataRowIterator {
    private:
        vector<unique_ptr<ColumnIterator>> columns_;
        ParquetRowView view_;
    public:
        ParquetRowIterator(ParquetBlock &block, uint64_t colindices)
                : columns_(64 - __builtin_clzl(colindices)), view_(columns_) {
            Bitset bitset(colindices);
            while (bitset.hasNext()) {
                auto index = bitset.next();
                columns_[index] = (block.col(index));
            }
        }

        virtual ~ParquetRowIterator() {
            columns_.clear();
        }

        virtual DataRow &operator[](uint64_t index) override {
            view_.index_ = index;
            return view_;
        }

        virtual DataRow &next() override {
            view_.index_++;
            return view_;
        }

        uint64_t pos() override {
            return view_.index_;
        }
    };

    unique_ptr<DataRowIterator> ParquetBlock::rows() {
        return unique_ptr<DataRowIterator>(new ParquetRowIterator(*this, columns_));
    }

#define COL_BUF_SIZE 8

    const int8_t WIDTH[8] = {1, 4, 8, 0, 4, 8, sizeof(ByteArray), 0};

    class ParquetColumnIterator : public ColumnIterator {
    private:
        shared_ptr<ColumnReader> columnReader_;
        DataField dataField_;
        int64_t read_counter_;
        uint64_t pos_;
        uint64_t bufpos_;
        uint8_t width_;
        uint8_t *buffer_;
    public:
        ParquetColumnIterator(shared_ptr<ColumnReader> colReader) : columnReader_(colReader),
                                                                    dataField_(), read_counter_(0), pos_(-1),
                                                                    bufpos_(-8) {
            buffer_ = (uint8_t *) malloc(sizeof(ByteArray) * COL_BUF_SIZE);
            width_ = WIDTH[columnReader_->type()];
        }

        virtual ~ParquetColumnIterator() {
            free(buffer_);
        }

        virtual DataField &operator[](uint64_t idx) override {
            uint64_t *pointer = loadBuffer(idx);
            pos_ = idx;
            dataField_ = pointer;
            return dataField_;
        }

        virtual DataField &operator()(uint64_t idx) override {
            uint64_t *pointer = loadBufferRaw(idx);
            pos_ = idx;
            dataField_ = pointer;
            return dataField_;
        }

        virtual DataField &next() override {
            return (*this)[pos_ + 1];
        }

        uint64_t pos() override {
            return pos_;
        }

    protected:
        inline uint64_t *loadBuffer(uint64_t idx) {
            if (idx < bufpos_ + COL_BUF_SIZE) {
                return (uint64_t *) (buffer_ + width_ * (idx - bufpos_));
            } else {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatch(COL_BUF_SIZE, nullptr, nullptr, buffer_, &read_counter_);
                bufpos_ = idx;
                return (uint64_t *) buffer_;
            }
        }

        inline uint64_t *loadBufferRaw(uint64_t idx) {
            if (idx < bufpos_ + COL_BUF_SIZE) {
                return (uint64_t *) (buffer_ + sizeof(int32_t) * (idx - bufpos_));
            } else {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatchRaw(COL_BUF_SIZE, reinterpret_cast<uint32_t *>(buffer_), &read_counter_);
                bufpos_ = idx;
                return (uint64_t *) buffer_;
            }
        }
    };

    unique_ptr<ColumnIterator> ParquetBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new ParquetColumnIterator(rowGroup_->Column(col_index)));
    }

    shared_ptr<Block> ParquetBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(dynamic_pointer_cast<ParquetBlock>(this->shared_from_this()), mask);
    }

    ParquetTable::ParquetTable(const string &fileName, uint64_t columns) : columns_(columns) {
        fileReader_ = ParquetFileReader::OpenFile(fileName);
        if (!fileReader_) {
            throw std::invalid_argument("ParquetTable-Open: file not found");
        }
    }

    void ParquetTable::updateColumns(uint64_t columns) {
        columns_ = columns;
    }

    shared_ptr<ParquetTable> ParquetTable::Open(const string &filename, uint64_t columns) {
        return make_shared<ParquetTable>(filename, columns);
    }

    shared_ptr<ParquetTable> ParquetTable::Open(const string &filename, std::initializer_list<uint32_t> columns) {
        uint64_t ccs = 0;
        for (uint32_t c:columns) {
            ccs |= 1ul << c;
        }
        return Open(filename, ccs);
    }

    ParquetTable::~ParquetTable() {

    }

    shared_ptr<Stream<shared_ptr<Block>>> ParquetTable::blocks() {
        function<shared_ptr<Block>(const int &)> mapper = [=](const int &idx) {
            return this->createParquetBlock(idx);
        };
        uint32_t numRowGroups = fileReader_->metadata()->num_row_groups();
        auto stream = IntStream::Make(0, numRowGroups)->map(mapper);
        return stream;
    }

    uint32_t ParquetTable::numFields() {
        return __builtin_popcount(columns_);
    }

    shared_ptr<ParquetBlock> ParquetTable::createParquetBlock(const int &block_idx) {
        auto rowGroup = fileReader_->RowGroup(block_idx);
        return make_shared<ParquetBlock>(rowGroup, block_idx, columns_);
    }

    TableView::TableView(uint32_t num_fields, shared_ptr<Stream<shared_ptr<Block>>> stream)
            : num_fields_(num_fields), stream_(stream) {}

    shared_ptr<Stream<shared_ptr<Block>>> TableView::blocks() {
        return stream_;
    }

    uint32_t TableView::numFields() {
        return num_fields_;
    }

    shared_ptr<MemTable> MemTable::Make(uint8_t num_fields) {
        return shared_ptr<MemTable>(new MemTable(num_fields));
    }

    MemTable::MemTable(uint8_t num_fields) : num_fields_(num_fields), blocks_(vector<shared_ptr<Block>>()) {}

    MemTable::~MemTable() {}

    uint32_t MemTable::numFields() {
        return num_fields_;
    }

    shared_ptr<MemBlock> MemTable::allocate(uint32_t num_rows) {
        shared_ptr<MemBlock> block = make_shared<MemBlock>(num_rows, num_fields_);
        blocks_.push_back(block);
        return block;
    }

    shared_ptr<Stream<shared_ptr<Block>>> MemTable::blocks() {
        return shared_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_));
    }

/**
 * Initialize the templates
 */
    template
    class Dictionary<Int32Type>;

    template
    class Dictionary<DoubleType>;

    template
    class Dictionary<ByteArrayType>;

    template
    class RawAccessor<Int32Type>;

    template
    class RawAccessor<DoubleType>;

    template
    class RawAccessor<ByteArrayType>;

    template shared_ptr<Bitmap> ParquetBlock::raw<Int32Type>(uint32_t col_index, RawAccessor<Int32Type> *accessor);

    template shared_ptr<Bitmap> ParquetBlock::raw<DoubleType>(uint32_t col_index, RawAccessor<DoubleType> *accessor);

    template shared_ptr<Bitmap>
    ParquetBlock::raw<ByteArrayType>(uint32_t col_index, RawAccessor<ByteArrayType> *accessor);
}