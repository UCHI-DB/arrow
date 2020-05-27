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

    ostream &operator<<(ostream &os, MemDataRow &dt) {
        uint64_t *raw = dt.raw();
        auto offsets = dt.offset();
        auto limit = dt.offset().size() - 1;
        for (uint32_t i = 0; i < limit; ++i) {
            auto size = offsets[i + 1] - offsets[i];
            if (size == 1) {
                os.write((char *) (raw + offsets[i]), 8);
            } else {
                auto value = dt[i].asByteArray();
                os << string((const char *) value.ptr, value.len);
            }
        }
        return os;
    }

    ostream &operator<<(ostream &os, DataField &dt) {
        if (dt.size_ == 1) {
            os.write((char *) dt.data(), 8);
        } else {
            auto value = dt.asByteArray();
            os.write((const char *) value.ptr, value.len);
        }
        return os;
    }

    mt19937 Block::rand_ = mt19937(time(NULL));

    const array<vector<uint32_t>, 11> OFFSETS = {
            vector<uint32_t>({0}),
            {0, 1},
            {0, 1, 2},
            {0, 1, 2, 3},
            {0, 1, 2, 3, 4},
            {0, 1, 2, 3, 4, 5},
            {0, 1, 2, 3, 4, 5, 6},
            {0, 1, 2, 3, 4, 5, 6, 7},
            {0, 1, 2, 3, 4, 5, 6, 7, 8},
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
            {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
    };

    const array<vector<uint32_t>, 11> SIZES = {
            vector<uint32_t>({}),
            vector<uint32_t>({1}),
            vector<uint32_t>({1, 1}),
            vector<uint32_t>({1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1, 1, 1}),
            vector<uint32_t>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
    };

    const vector<uint32_t> &colOffset(uint32_t num_fields) {
        return OFFSETS[num_fields];
    }

    const vector<uint32_t> &colSize(uint32_t num_fields) {
        return SIZES[num_fields];
    }

    MemDataRow MemDataRow::EMPTY = MemDataRow(0);

    MemDataRow::MemDataRow(uint8_t num_fields)
            : MemDataRow(OFFSETS[num_fields]) {}

    MemDataRow::MemDataRow(const vector<uint32_t> &offset) : data_(offset.back(), 0x0), offset_(offset) {}

    unique_ptr<DataRow> MemDataRow::snapshot() {
        MemDataRow *mdr = new MemDataRow(offset_);
        *mdr = *this;
        return unique_ptr<DataRow>(mdr);
    }

    DataField &MemDataRow::operator[](uint64_t i) {
        view_ = data_.data() + offset_[i];
        assert(i + 1 < offset_.size());
        view_.size_ = offset_[i + 1] - offset_[i];
        return view_;
    }

    void MemDataRow::operator=(DataRow &row) {
        if (row.raw()) {
            memcpy(static_cast<void *>(data_.data()), static_cast<void *>(row.raw()),
                   sizeof(uint64_t) * data_.size());
        } else {
            auto offset_size = offset_.size();
            for (uint32_t i = 0; i < offset_size - 1; ++i) {
                (*this)[i] = row[i];
            }
        }
    }

    MemDataRow &MemDataRow::operator=(MemDataRow &row) {
        memcpy(static_cast<void *>(data_.data()), static_cast<void *>(row.raw()),
               sizeof(uint64_t) * data_.size());
        return *this;
    }

    uint64_t *MemDataRow::raw() {
        return data_.data();
    }

    MemBlock::MemBlock(uint32_t size, uint32_t row_size, const vector<uint32_t> &col_offset)
            : size_(size), row_size_(row_size), col_offset_(col_offset) {
        content_ = vector<uint64_t>(size * row_size_);
    }

    MemBlock::MemBlock(uint32_t size, uint32_t row_size) : MemBlock(size, row_size, OFFSETS[row_size]) {}

    uint64_t MemBlock::size() {
        return size_;
    }

    void MemBlock::resize(uint32_t newsize) {
        content_.resize(newsize * row_size_);
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
        uint32_t row_size_;
        const vector<uint32_t> &col_offset_;
        DataField view_;
        friend MemDataRowIterator;
    public:
        MemDataRowView(vector<uint64_t> &data, uint32_t row_size, const vector<uint32_t> &col_offset)
                : data_(data), index_(-1), row_size_(row_size), col_offset_(col_offset) {}

        virtual ~MemDataRowView() {}

        void moveto(uint64_t index) { index_ = index; }

        void next() { ++index_; }

        unique_ptr<DataRow> snapshot() override {
            MemDataRowView *snapshot = new MemDataRowView(data_, row_size_, col_offset_);
            snapshot->index_ = index_;
            return unique_ptr<DataRow>(snapshot);
        }

        DataField &operator[](uint64_t i) override {
            assert(index_ < data_.size() / row_size_);
            assert(col_offset_[i] < row_size_);
            view_ = data_.data() + index_ * row_size_ + col_offset_[i];
            view_.size_ = col_offset_[i + 1] - col_offset_[i];
            return view_;
        }

        uint64_t *raw() override {
            return data_.data() + index_ * row_size_;
        }

        void operator=(DataRow &row) override {
            memcpy(static_cast<void *>(data_.data() + index_ * row_size_), static_cast<void *>(row.raw()),
                   sizeof(uint64_t) * row_size_);
        }
    };

    class MemDataRowIterator : public DataRowIterator {
    private:
        MemDataRowView reference_;
    public:
        MemDataRowIterator(vector<uint64_t> &data, uint32_t row_size, const vector<uint32_t> &col_offset)
                : reference_(data, row_size, col_offset) {}

        DataRow &operator[](uint64_t idx) override {
            reference_.moveto(idx);
            return reference_;
        }

        DataRow &next() override {
            reference_.next();
            return reference_;
        }

        uint64_t pos() override {
            return reference_.index_;
        }
    };

    class MemColumnIterator : public ColumnIterator {
    private:
        vector<uint64_t> &data_;
        uint32_t row_size_;
        uint32_t col_offset_;
        uint64_t row_index_;
        DataField view_;
    public:
        MemColumnIterator(vector<uint64_t> &data, uint32_t row_size, uint32_t col_offset, uint32_t col_size)
                : data_(data), row_size_(row_size), col_offset_(col_offset), row_index_(-1) {
            view_.size_ = col_size;
        }

        DataField &operator[](uint64_t idx) override {
            assert(idx < data_.size() / row_size_);
            row_index_ = idx;
            view_ = data_.data() + idx * row_size_ + col_offset_;
            return view_;
        }

        DataField &next() override {
            view_ = data_.data() + (++row_index_) * row_size_ + col_offset_;
            return view_;
        }

        uint64_t pos() override {
            return row_index_;
        }
    };

    unique_ptr<DataRowIterator> MemBlock::rows() {
        return unique_ptr<DataRowIterator>(new MemDataRowIterator(content_, row_size_, col_offset_));
    }

    unique_ptr<ColumnIterator> MemBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new MemColumnIterator(content_, row_size_, col_offset_[col_index],
                                                                col_offset_[col_index + 1] - col_offset_[col_index]));
    }

    shared_ptr<Block> MemBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

    MemvBlock::MemvBlock(uint32_t size, const vector<uint32_t> &col_size) : size_(size), col_size_(col_size) {
        uint8_t num_fields = col_size.size();
        for (uint8_t i = 0; i < num_fields; ++i) {
            content_.push_back(unique_ptr<vector<uint64_t>>(new vector<uint64_t>(size * col_size_[i])));
        }
    }

    MemvBlock::MemvBlock(uint32_t size, uint32_t num_fields) : MemvBlock(size, SIZES[num_fields]) {}

    uint64_t MemvBlock::size() {
        return size_;
    }

    void MemvBlock::resize(uint32_t newsize) {
        size_ = newsize;
        uint8_t num_fields = col_size_.size();
        for (uint8_t i = 0; i < num_fields; ++i) {
            content_[i]->resize(size_ * col_size_[i]);
        }
    }

    class MemvColumnIterator : public ColumnIterator {
    private:
        vector<uint64_t> &data_;
        uint64_t row_index_;
        DataField view_;
    public:
        MemvColumnIterator(vector<uint64_t> &data, uint32_t col_size)
                : data_(data), row_index_(-1) {
            assert(col_size <= 2);
            view_.size_ = col_size;
        }

        DataField &operator[](uint64_t idx) override {
            assert(idx < data_.size() / view_.size_);
            row_index_ = idx;
            view_ = data_.data() + idx * view_.size_;
            return view_;
        }

        DataField &next() override {
            ++row_index_;
            view_ = data_.data() + row_index_ * view_.size_;
            return view_;
        }

        uint64_t pos() override {
            return row_index_;
        }
    };

    class MemvDataRowIterator;

    class MemvDataRowView : public DataRow {
    private:
        vector<unique_ptr<vector<uint64_t>>> *cols_;
        const vector<uint32_t> &col_size_;
        uint64_t index_;
        DataField view_;

        friend MemvDataRowIterator;
    public:
        MemvDataRowView(vector<unique_ptr<vector<uint64_t>>> *cols, const vector<uint32_t> &col_sizes)
                : cols_(cols), col_size_(col_sizes), index_(-1) {
        }

        virtual ~MemvDataRowView() {}

        void moveto(uint64_t index) {
            index_ = index;
        }

        void next() {
            ++index_;
        }

        DataField &operator[](uint64_t i) override {
            view_.size_ = col_size_[i];
            view_ = (*cols_)[i]->data() + view_.size_ * index_;
            return view_;
        }

        void operator=(DataRow &row) override {
            uint32_t num_cols = cols_->size();
            for (uint32_t i = 0; i < num_cols; ++i) {
                (*this)[i] = row[i];
            }
        }

        unique_ptr<DataRow> snapshot() override {
            MemvDataRowView *view = new MemvDataRowView(cols_, col_size_);
            view->index_ = index_;
            return unique_ptr<DataRow>(view);
        }
    };

    class MemvDataRowIterator : public DataRowIterator {
    private:
        MemvDataRowView reference_;
    public:
        MemvDataRowIterator(vector<unique_ptr<vector<uint64_t>>> *cols, const vector<uint32_t> &col_sizes)
                : reference_(cols, col_sizes) {}

        DataRow &operator[](uint64_t idx) override {
            reference_.moveto(idx);
            return reference_;
        }

        DataRow &next() override {
            reference_.next();
            return reference_;
        }

        uint64_t pos() override {
            return reference_.index_;
        }
    };


    unique_ptr<DataRowIterator> MemvBlock::rows() {
        auto cols = new vector<unique_ptr<ColumnIterator>>();
        auto col_size = col_size_.size();
        for (auto i = 0u; i < col_size; ++i) {
            cols->push_back(col(i));
        }
        return unique_ptr<DataRowIterator>(new MemvDataRowIterator(&content_, col_size_));
    }

    unique_ptr<ColumnIterator> MemvBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new MemvColumnIterator(*content_[col_index], col_size_[col_index]));
    }

    shared_ptr<Block> MemvBlock::mask(shared_ptr<Bitmap> mask) {
        // Does not support
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

    void MemvBlock::merge(MemvBlock &another, const vector<pair<uint8_t, uint8_t>> &merge_inst) {
        this->size_ = std::max(size_, another.size_);
        for (auto &inst: merge_inst) {
            content_[inst.second] = move(another.content_[inst.first]);
        }
        // The old memblock is discarded
        another.content_.clear();
    }

    MemListBlock::MemListBlock() {}

    MemListBlock::~MemListBlock() {
        for (auto &item: content_) {
            delete item;
        }
    }

    uint64_t MemListBlock::size() {
        return content_.size();
    }

    class MemListColumnIterator : public ColumnIterator {
    protected:
        vector<DataRow *> &ref_;
        uint32_t col_index_;
        uint32_t index_;
    public:
        MemListColumnIterator(vector<DataRow *> &ref, uint32_t col_index)
                : ref_(ref), col_index_(col_index), index_(-1) {}

        DataField &next() override {
            index_++;
            return (*ref_[index_])[col_index_];
        }

        uint64_t pos() override {
            return index_;
        }

        DataField &operator[](uint64_t index) override {
            index_ = index;
            return (*ref_[index_])[col_index_];
        }
    };

    unique_ptr<ColumnIterator> MemListBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new MemListColumnIterator(content_, col_index));
    }

    class MemListRowIterator : public DataRowIterator {
    protected:
        vector<DataRow *> &ref_;
        uint32_t index_;
    public:
        MemListRowIterator(vector<DataRow *> &ref) : ref_(ref), index_(-1) {}

        DataRow &next() override {
            index_++;
            return *(ref_[index_]);
        }

        uint64_t pos() override {
            return index_;
        }

        DataRow &operator[](uint64_t index) override {
            index_ = index;
            return *(ref_[index_]);
        }
    };

    unique_ptr<DataRowIterator> MemListBlock::rows() {
        return unique_ptr<DataRowIterator>(new MemListRowIterator(content_));
    }

    shared_ptr<Block> MemListBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

    MaskedBlock::MaskedBlock(shared_ptr<Block> inner, shared_ptr<Bitmap> mask)
            : inner_(inner), mask_(mask) {}

    uint64_t MaskedBlock::size() {
        return mask_->cardinality();
    }

    uint64_t MaskedBlock::limit() {
        return inner_->limit();
    }

    class MaskedColumnIterator : public ColumnIterator {
    private:
        unique_ptr<ColumnIterator> inner_;
        unique_ptr<BitmapIterator> bite_;
    public:
        MaskedColumnIterator(unique_ptr<ColumnIterator> inner, unique_ptr<BitmapIterator> bite)
                : inner_(move(inner)), bite_(move(bite)) {}

        DataField &operator[](uint64_t index) override {
            return (*inner_)[index];
        }

        DataField &next() override {
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
        /// This sequence makes sure that we do not change the original mask, which may be shared by
        /// other instances of MaskedBlock
        this->mask_ = (*mask) & (*this->mask_);
        return this->shared_from_this();
    }

    ParquetBlock::ParquetBlock(ParquetTable *owner, shared_ptr<RowGroupReader> rowGroup, uint32_t index,
                               uint64_t columns) : Block(index), owner_(owner), rowGroup_(rowGroup), index_(index),
                                                   columns_(columns) {}

    Table *ParquetBlock::owner() {
        return this->owner_;
    }

    uint64_t ParquetBlock::size() {
        return rowGroup_->metadata()->num_rows();
    }

    class ParquetRowIterator;

#define COL_BUF_SIZE 8
    const int8_t WIDTH[8] = {1, sizeof(int32_t), sizeof(int64_t), 0, sizeof(float), sizeof(double), sizeof(ByteArray),
                             0};
    const int8_t SIZE[8] = {1, 1, 1, 1, 1, 1, sizeof(ByteArray) >> 3, 0};

    class ParquetColumnIterator : public ColumnIterator {
    private:
        shared_ptr<ColumnReader> columnReader_;
        DataField dataField_;
        DataField rawField_;
        int64_t buffer_size_;
        int64_t pos_;
        int64_t bufpos_;
        uint8_t width_;
        uint8_t *buffer_;
    public:
        ParquetColumnIterator(shared_ptr<ColumnReader> colReader)
                : columnReader_(colReader), dataField_(), rawField_(),
                  buffer_size_(0), pos_(-1), bufpos_(-8) {
            buffer_ = (uint8_t *) malloc(sizeof(ByteArray) * COL_BUF_SIZE);
            width_ = WIDTH[columnReader_->type()];
            dataField_.size_ = SIZE[columnReader_->type()];
            rawField_.size_ = 1;
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
            rawField_ = pointer;
            return rawField_;
        }

        virtual DataField &next() override {
            return (*this)[pos_ + 1];
        }

        uint64_t pos() override {
            return pos_;
        }

        const uint8_t *dict() {
            return (const uint8_t *) columnReader_->dictionary();
        }

    protected:
        inline uint64_t *loadBuffer(uint64_t idx) {
            if ((int64_t) idx < bufpos_ + buffer_size_) {
                return (uint64_t *) (buffer_ + width_ * (idx - bufpos_));
            } else {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatch(COL_BUF_SIZE, nullptr, nullptr, buffer_, &buffer_size_);
                bufpos_ = idx;
                return (uint64_t *) buffer_;
            }
        }

        inline uint64_t *loadBufferRaw(uint64_t idx) {
            if ((int64_t) idx < bufpos_ + buffer_size_) {
                return (uint64_t *) (buffer_ + sizeof(int32_t) * (idx - bufpos_));
            } else {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatchRaw(COL_BUF_SIZE, reinterpret_cast<uint32_t *>(buffer_), &buffer_size_);
                bufpos_ = idx;

                return (uint64_t *) buffer_;
            }
        }
    };

    class ParquetRowView : public DataRow {
    protected:
        vector<unique_ptr<ParquetColumnIterator>> &columns_;
        uint64_t index_;
        friend ParquetRowIterator;
    public:
        ParquetRowView(vector<unique_ptr<ParquetColumnIterator>> &cols) : columns_(cols), index_(-1) {}

        virtual DataField &operator[](uint64_t colindex) override {
            return (*(columns_[colindex]))[index_];
        }

        virtual DataField &operator()(uint64_t colindex) override {
            return (*(columns_[colindex]))(index_);
        }

        unique_ptr<DataRow> snapshot() override {
            return nullptr;
        }
    };

    template<typename DTYPE>
    shared_ptr<Bitmap> ParquetBlock::raw(uint32_t col_index, RawAccessor<DTYPE> *accessor) {
        accessor->init(this->size());
        auto pageReader = rowGroup_->GetColumnPageReader(col_index);
        shared_ptr<Page> page = pageReader->NextPage();

        if (page->type() == PageType::DICTIONARY_PAGE) {
            Dictionary<DTYPE> dict(static_pointer_cast<DictionaryPage>(page));
            accessor->dict(dict);
        } else {
            accessor->data((DataPage *) page.get());
        }
        while ((page = pageReader->NextPage())) {
            accessor->data((DataPage *) page.get());
        }
        return accessor->result();
    }

    class ParquetRowIterator : public DataRowIterator {
    private:
        vector<unique_ptr<ParquetColumnIterator>> columns_;
        ParquetRowView view_;
    public:
        ParquetRowIterator(ParquetBlock &block, uint64_t colindices)
                : columns_(64 - __builtin_clzl(colindices)), view_(columns_) {
            Bitset bitset(colindices);
            while (bitset.hasNext()) {
                auto index = bitset.next();
                columns_[index] = unique_ptr<ParquetColumnIterator>(
                        (ParquetColumnIterator *) (block.col(index).release()));
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

        const uint8_t *dict(uint32_t idx) override {
            return columns_[idx]->dict();
        }
    };

    unique_ptr<DataRowIterator> ParquetBlock::rows() {
        return unique_ptr<DataRowIterator>(new ParquetRowIterator(*this, columns_));
    }

    unique_ptr<ColumnIterator> ParquetBlock::col(uint32_t col_index) {
        return unique_ptr<ColumnIterator>(new ParquetColumnIterator(rowGroup_->Column(col_index)));
    }

    shared_ptr<Block> ParquetBlock::mask(shared_ptr<Bitmap> mask) {
        return make_shared<MaskedBlock>(dynamic_pointer_cast<ParquetBlock>(this->shared_from_this()), mask);
    }

    uint64_t Table::size() {
        function<uint64_t(const shared_ptr<Block> &)> sizer = [](const shared_ptr<Block> &block) {
            return block->size();
        };
        function<uint64_t(uint64_t, uint64_t)> reducer = [](uint64_t a, uint64_t b) {
            return a + b;
        };
        return blocks()->map(sizer)->reduce(reducer);
    }

    ParquetTable::ParquetTable(const string &fileName, uint64_t columns) : name_(fileName), columns_(columns) {
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

    using namespace std::placeholders;

    unique_ptr<Stream<shared_ptr<Block>>> ParquetTable::blocks() {
        function<shared_ptr<Block>(const int &)> mapper = bind(&ParquetTable::createParquetBlock, this, _1);
        uint32_t numRowGroups = fileReader_->metadata()->num_row_groups();
#ifdef LQF_PARALLEL
        auto stream = IntStream::Make(0, numRowGroups)->parallel()->map(mapper);
#else
        auto stream = IntStream::Make(0, numRowGroups)->map(mapper);
#endif
        return stream;
    }

    const vector<uint32_t> &ParquetTable::colSize() {
        return lqf::colSize(0);
    }

    shared_ptr<ParquetBlock> ParquetTable::createParquetBlock(const int &block_idx) {
        auto rowGroup = fileReader_->RowGroup(block_idx);
        return make_shared<ParquetBlock>(this, rowGroup, block_idx, columns_);
    }

    template<typename DTYPE>
    unique_ptr<Dictionary<DTYPE>> ParquetTable::LoadDictionary(int column) {
        auto dictpage = static_pointer_cast<DictionaryPage>(
                fileReader_->RowGroup(0)->GetColumnPageReader(column)->NextPage());
        return unique_ptr<Dictionary<DTYPE>>(new Dictionary<DTYPE>(dictpage));
    }

    uint32_t ParquetTable::DictionarySize(int column) {
        auto dictpage = static_pointer_cast<DictionaryPage>(
                fileReader_->RowGroup(0)->GetColumnPageReader(column)->NextPage());
        return dictpage->num_values();
    }

    MaskedTable::MaskedTable(ParquetTable *inner, vector<shared_ptr<Bitmap>> &masks)
            : inner_(inner), masks_(masks) {
        masks_.resize(inner_->numBlocks());
    }

    using namespace std::placeholders;

    unique_ptr<Stream<shared_ptr<Block>>> MaskedTable::blocks() {
        function<shared_ptr<Block>(const shared_ptr<Block> &)> mapper =
                bind(&MaskedTable::buildMaskedBlock, this, _1);
        return inner_->blocks()->map(mapper);
    }

    const vector<uint32_t> &MaskedTable::colSize() {
        return inner_->colSize();
    }

    shared_ptr<Block> MaskedTable::buildMaskedBlock(const shared_ptr<Block> &input) {
        auto pblock = dynamic_pointer_cast<ParquetBlock>(input);
        return make_shared<MaskedBlock>(pblock, masks_[pblock->index()]);
    }

    TableView::TableView(const vector<uint32_t> &col_size, unique_ptr<Stream<shared_ptr<Block>>> stream)
            : col_size_(col_size), stream_(move(stream)) {}

    const vector<uint32_t> &TableView::colSize() {
        return col_size_;
    }

    unique_ptr<Stream<shared_ptr<Block>>> TableView::blocks() {
        return move(stream_);
    }

    shared_ptr<MemTable> MemTable::Make(uint8_t num_fields, bool vertical) {
        return shared_ptr<MemTable>(new MemTable(lqf::colSize(num_fields), vertical));
    }

    shared_ptr<MemTable> MemTable::Make(const vector<uint32_t> col_size, bool vertical) {
        return shared_ptr<MemTable>(new MemTable(col_size, vertical));
    }

    MemTable::MemTable(const vector<uint32_t> col_size, bool vertical)
            : vertical_(vertical), col_size_(col_size), row_size_(0), blocks_(vector<shared_ptr<Block>>()) {
        col_offset_.push_back(0);
        auto num_fields = col_size_.size();
        for (uint8_t k = 0; k < num_fields; ++k) {
            row_size_ += col_size_[k];
            col_offset_.push_back(col_offset_.back() + col_size_[k]);
        }
    }

    shared_ptr<Block> MemTable::allocate(uint32_t num_rows) {
        shared_ptr<Block> block;
        if (vertical_)
            block = make_shared<MemvBlock>(num_rows, col_size_);
        else
            block = make_shared<MemBlock>(num_rows, row_size_, col_offset_);

        std::unique_lock lock(write_lock_);
        blocks_.push_back(block);
        lock.unlock();

        return block;
    }

    void MemTable::append(shared_ptr<Block> block) {
        std::lock_guard lock(write_lock_);
        blocks_.push_back(block);
    }

    unique_ptr<Stream<shared_ptr<Block>>> MemTable::blocks() {
#ifdef LQF_PARALLEL
        return unique_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_))->parallel();
#else
        return unique_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_));
#endif
    }

    const vector<uint32_t> &MemTable::colSize() { return col_size_; }

    const vector<uint32_t> &MemTable::colOffset() { return col_offset_; }

/**
 * Initialize the templates
 */

    template
    class RawAccessor<Int32Type>;

    template
    class RawAccessor<DoubleType>;

    template
    class RawAccessor<ByteArrayType>;

    template shared_ptr<Bitmap>
    ParquetBlock::raw<Int32Type>(uint32_t col_index, RawAccessor<Int32Type> *accessor);

    template shared_ptr<Bitmap>
    ParquetBlock::raw<DoubleType>(uint32_t col_index, RawAccessor<DoubleType> *accessor);

    template shared_ptr<Bitmap>
    ParquetBlock::raw<ByteArrayType>(uint32_t col_index, RawAccessor<ByteArrayType> *accessor);

    template unique_ptr<Dictionary<Int32Type>> ParquetTable::LoadDictionary<Int32Type>(int index);

    template unique_ptr<Dictionary<DoubleType>> ParquetTable::LoadDictionary<DoubleType>(int index);

    template unique_ptr<Dictionary<ByteArrayType>> ParquetTable::LoadDictionary<ByteArrayType>(int index);
}