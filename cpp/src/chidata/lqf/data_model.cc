//
// Created by harper on 2/9/20.
//

#include <exception>
#include "data_model.h"
#include <parquet/column_reader.h>

namespace chidata {
    namespace lqf {
        using namespace std;
        using namespace chidata;


        MemBlock::MemBlock(uint32_t size, uint8_t num_fields) : size_(size), num_fields_(num_fields) {
            content_ = vector<MemDataField>(size * num_fields);
        }

        MemBlock::~MemBlock() {
        }

        uint64_t MemBlock::size() {
            return size_;
        }

        void MemBlock::inc(uint32_t row_to_inc) {
            content_.resize(content_.size() + num_fields_ * row_to_inc);
        }

        class MemDataRowView : public DataRow {
        private:
            vector<MemDataField> &data_;
            uint64_t index_;
            uint8_t num_fields_;
        public:
            MemDataRowView(vector<MemDataField> &data, uint8_t num_fields)
                    : data_(data), num_fields_(num_fields) {}

            virtual ~MemDataRowView() {

            }

            void moveto(uint64_t index) {
                index_ = index;
            }

            void next() {
                ++index_;
            }

            virtual DataField &operator[](uint64_t i) override {
                return data_[index_ * num_fields_ + i];
            }
        };

        class MemDataRowIterator : public DataRowIterator {
        private:
            MemDataRowView reference_;
        public:
            MemDataRowIterator(vector<MemDataField> &data, uint8_t num_fields)
                    : reference_(data, num_fields) {}

            virtual DataRow &operator[](uint64_t idx) override {
                reference_.moveto(idx);
                return reference_;
            }

            virtual DataRow &next() override {
                reference_.next();
                return reference_;
            }
        };

        class MemColumnIterator : public ColumnIterator {
        private:
            uint8_t num_fields_;
            vector<MemDataField> &data_;
            uint8_t colindex_;
            uint64_t rowindex_;
        public:
            MemColumnIterator(vector<MemDataField> &data, uint8_t num_fields, uint8_t colindex)
                    : num_fields_(num_fields), data_(data), colindex_(colindex), rowindex_(0) {}

            virtual DataField &operator[](uint64_t idx) override {
                rowindex_ = idx;
                return data_[idx * num_fields_ + colindex_];
            }

            DataField &next() override {
                ++rowindex_;
                return data_[rowindex_ * num_fields_ + colindex_];
            }

        };

        unique_ptr<DataRowIterator> MemBlock::rows() {
            return unique_ptr<DataRowIterator>(new MemDataRowIterator(content_, num_fields_));
        }

        unique_ptr<ColumnIterator> MemBlock::col(uint32_t col_index) {
            return unique_ptr<ColumnIterator>(new MemColumnIterator(content_, num_fields_, col_index));
        }

        shared_ptr<Block> MemBlock::mask(shared_ptr<Bitmap> &mask) {
            throw std::invalid_argument("unsupported");
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
        };

        unique_ptr<DataRowIterator> MaskedBlock::rows() {
            return unique_ptr<MaskedRowIterator>(new MaskedRowIterator(inner_->rows(), mask_->iterator()));
        }

        shared_ptr<Block> MaskedBlock::mask(shared_ptr<Bitmap> &mask) {
            this->mask_ = (*this->mask_) & *mask;
            return this->shared_from_this();
        }

        ParquetBlock::ParquetBlock(shared_ptr<RowGroupReader> rowGroup, uint32_t index, uint64_t columns)
                : rowGroup_(rowGroup), index_(index), columns_(columns) {}

        ParquetBlock::~ParquetBlock() {

        }

        uint64_t ParquetBlock::size() {
            return rowGroup_->metadata()->num_rows();
        }

        class ParquetRowView : public DataRow {
        protected:
            vector<unique_ptr<ColumnIterator>> columns_;
            uint64_t index_;
        public:
            ParquetRowView(vector<unique_ptr<ColumnIterator>> &cols) : columns_(cols), index_(0) {}

            virtual DataField &operator[](uint64_t colindex) override {
                return (*columns_[index_])[colindex];
            }

            void setIndex(uint64_t index) {
                this->index_ = index;
            }
        };

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
                view_.setIndex(index);
                return view_;
            }

            virtual DataRow &next() override {
                auto it = columns_.begin();
                while (it < columns_.end()) {
                    (*it)->next();
                    it++;
                }
                return view_;
            }
        };

        unique_ptr<DataRowIterator> ParquetBlock::rows() {
            return unique_ptr<DataRowIterator>(new ParquetRowIterator(*this, columns_));
        }

        class ParquetColumnIterator : public ColumnIterator, DataField {
        private:
            shared_ptr<ColumnReader> columnReader_;
            MemDataField dataField_;
            int64_t read_counter_;
            bool consumed_;
        public:
            ParquetColumnIterator(shared_ptr<ColumnReader> colReader) : columnReader_(colReader), dataField_(),
                                                                        read_counter_(0), consumed_(0) {}

            virtual ~ParquetColumnIterator() {}

            virtual DataField &operator[](uint64_t idx) override {
                columnReader_->MoveTo(idx);
                consumed_ = false;
                return *this;
            }

            virtual DataField &next() override {
                // Do nothing as the asXX method will automatically move forward
                if (!consumed_) {
                    columnReader_->Skip(1);
                }
                consumed_ = false;
            }

            virtual int32_t asInt() override {
                auto res = static_pointer_cast<Int32Reader>(columnReader_);
                res->ReadBatch(1, nullptr, nullptr, (int32_t *) (dataField_.data()), &read_counter_);
                consumed_ = true;
                return dataField_.asInt();
            }

            virtual double asDouble() override {
                auto res = static_pointer_cast<DoubleReader>(columnReader_);
                res->ReadBatch(1, nullptr, nullptr, (double *) (dataField_.data()), &read_counter_);
                consumed_ = true;
                return dataField_.asInt();
            }

            virtual void *asString() override {
                auto res = static_pointer_cast<ByteArrayReader>(columnReader_);
                throw invalid_argument("");
            };

            virtual void operator=(int32_t) override {
                throw invalid_argument("");
            }

            virtual void operator=(double) override {
                throw invalid_argument("");
            }

            virtual void operator=(void *) override {
                throw invalid_argument("");
            }
        };

        unique_ptr<ColumnIterator> ParquetBlock::col(uint32_t col_index) {
            return unique_ptr<ColumnIterator>(new ParquetColumnIterator(rowGroup_->Column(col_index)));
        }

        shared_ptr<Block> ParquetBlock::mask(shared_ptr<Bitmap> &mask) {
            return make_shared<MaskedBlock>(dynamic_pointer_cast<ParquetBlock>(this->shared_from_this()), mask);
        }

        ParquetTable::ParquetTable(const string &fileName) {
            fileReader_ = ParquetFileReader::OpenFile(fileName);
        }

        ParquetTable::~ParquetTable() {

        }

        shared_ptr<Stream<shared_ptr<Block>>>

        ParquetTable::blocks() {
            function<shared_ptr<Block>(const int &)> mapper = [=](const int &idx) {
                return this->createParquetBlock(idx);
            };
            auto stream = IntStream::Make(0, 10)->map(mapper);
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