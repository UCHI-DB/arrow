//
// Created by harper on 2/9/20.
//

#include <exception>
#include "data_model.h"
#include <parquet/column_reader.h>
#include <chidata/validate.h>
#include <parquet/encoding.h>
#include <arrow/util/bit_stream_utils.h>

namespace chidata {
    namespace lqf {
        using namespace std;
        using namespace chidata;


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

            virtual DataField &operator[](uint64_t i) override {
                view_ = data_.data() + index_ * num_fields_ + i;
                return view_;
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
        Dictionary<DTYPE>::Dictionary(DictionaryPage *dpage):buffer_() {
            auto decoder = parquet::MakeTypedDecoder<DTYPE>(Encoding::PLAIN, nullptr);
            decoder->SetData(dpage->num_values(), dpage->data(), dpage->size());
            buffer_.resize(dpage->num_values());
            decoder->Decode(buffer_.data(), buffer_.size());
        }

        template<typename DTYPE>
        uint32_t Dictionary<DTYPE>::lookup(T key) {
            uint32_t low = 0;
            uint32_t high = buffer_.size();

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

        template<typename DTYPE>
        uint8_t *RawAccessor<DTYPE>::data(DataPage *dpage) {
            uint8_t *data_base = const_cast<uint8_t *>(dpage->data());
            uint64_t offset = 0;
            uint64_t buffer = 0;
            arrow::BitUtil::BitReader reader(data_base, dpage->size());
            if (dpage->type() == PageType::DATA_PAGE) {
                reader.GetValue(32, &buffer);
                offset += buffer + 4;
                reader.Skip(8, buffer);
                reader.GetValue(32, &buffer);
                offset += buffer + 4;
            } else {
                throw invalid_argument("DataPageV2 not supported");
            }
            return data_base + offset;
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
                validate_true(colindex < columns_.size(), "column not available");
                return (*(columns_[colindex]))[index_];
            }

            void setIndex(uint64_t index) {
                this->index_ = index;
            }
        };

        template<typename DTYPE>
        shared_ptr<Bitmap> ParquetBlock::raw(uint32_t col_index, RawAccessor<DTYPE> *accessor) {
            auto pageReader = rowGroup_->GetColumnPageReader(col_index);

            shared_ptr<Dictionary<DTYPE>> dict = nullptr;
            shared_ptr<Page> page = nullptr;
            shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(size());
            while ((page = pageReader->NextPage())) {
                if (page->type() == PageType::DICTIONARY_PAGE) {
                    accessor->dict((DictionaryPage *) page.get());
                } else {
                    accessor->filter((DataPage *) page.get(), bitmap);
                }
            }
            return bitmap;
        }

        class ParquetColumnIterator;

        class ParquetRowIterator : public DataRowIterator {
        private:
            vector<unique_ptr<ColumnIterator>> columns_;
            ParquetRowView view_;
        public:
            ParquetRowIterator(ParquetBlock &block, uint64_t colindices)
                    : columns_(), view_(columns_) {
                Bitset bitset(colindices);
                while (bitset.hasNext()) {
                    columns_.push_back(block.col(bitset.next()));
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
                return columns_[0]->pos();
            }
        };

        unique_ptr<DataRowIterator> ParquetBlock::rows() {
            return unique_ptr<DataRowIterator>(new ParquetRowIterator(*this, columns_));
        }

        class ParquetColumnIterator : public ColumnIterator {
        private:
            shared_ptr<ColumnReader> columnReader_;
            uint64_t buffer_;
            DataField dataField_;
            int64_t read_counter_;
            uint64_t pos_;
        public:
            ParquetColumnIterator(shared_ptr<ColumnReader> colReader) : columnReader_(colReader), buffer_(),
                                                                        dataField_(), read_counter_(0), pos_(-1) {
                dataField_ = &buffer_;
            }

            virtual ~ParquetColumnIterator() {}

            virtual DataField &operator[](uint64_t idx) override {
                columnReader_->MoveTo(idx);
                columnReader_->ReadBatch(1, nullptr, nullptr, (void *) dataField_.data(), &read_counter_);
                pos_ = idx;
                return dataField_;
            }

            virtual DataField &next() override {
                columnReader_->ReadBatch(1, nullptr, nullptr, (void *) dataField_.data(), &read_counter_);
                ++pos_;
                return dataField_;
            }

            uint64_t pos() override {
                return pos_;
            }
        };

        unique_ptr<ColumnIterator> ParquetBlock::col(uint32_t col_index) {
            return unique_ptr<ColumnIterator>(new ParquetColumnIterator(rowGroup_->Column(col_index)));
        }

        shared_ptr<Block> ParquetBlock::mask(shared_ptr<Bitmap> mask) {
            return make_shared<MaskedBlock>(dynamic_pointer_cast<ParquetBlock>(this->shared_from_this()), mask);
        }

        ParquetTable::ParquetTable(const string &fileName) {
            fileReader_ = ParquetFileReader::OpenFile(fileName);
        }

        void ParquetTable::updateColumns(uint64_t columns) {
            columns_ = columns;
        }

        shared_ptr<ParquetTable> ParquetTable::Open(const string &filename) {
            return make_shared<ParquetTable>(filename);
        }

        ParquetTable::~ParquetTable() {

        }

        shared_ptr<Stream<shared_ptr<Block>>>

        ParquetTable::blocks() {
            function<shared_ptr<Block>(const int &)> mapper = [=](const int &idx) {
                return this->createParquetBlock(idx);
            };
            uint32_t numRowGroups = fileReader_->metadata()->num_row_groups();
            auto stream = IntStream::Make(0, numRowGroups)->map(mapper);
            return stream;
        }

        shared_ptr<ParquetBlock> ParquetTable::createParquetBlock(const int &block_idx) {
            auto rowGroup = fileReader_->RowGroup(block_idx);
            return make_shared<ParquetBlock>(rowGroup, block_idx, columns_);
        }

        TableView::TableView(shared_ptr<Stream<shared_ptr<Block>>> stream)
                : stream_(stream) {}

        shared_ptr<Stream<shared_ptr<Block>>>

        TableView::blocks() {
            return stream_;
        }

        shared_ptr<MemTable> MemTable::Make(uint8_t num_fields) {
            return shared_ptr<MemTable>(new MemTable(num_fields));
        }

        MemTable::MemTable(uint8_t num_fields) : num_fields_(num_fields), blocks_(vector<shared_ptr<Block>>()) {

        }

        MemTable::~MemTable() {

        }

        shared_ptr<MemBlock> MemTable::allocate(uint32_t num_rows) {
            shared_ptr<MemBlock> block = make_shared<MemBlock>(num_rows, num_fields_);
            blocks_.push_back(block);
            return block;
        }

        shared_ptr<Stream<shared_ptr<Block>>>

        MemTable::blocks() {
            return shared_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_));
        }

    }
}