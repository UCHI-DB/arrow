//
// Created by harper on 2/9/20.
//

#include <exception>
#include "data_model.h"
#include <parquet/column_reader.h>

namespace chidata {
    namespace lqf {
        using namespace std;

        class MemDataField : public DataField {
        private:
            uint64_t value_;
        public:
            virtual int32_t asInt() override {
                return static_cast<int32_t>(value_);
            }

            virtual double asDouble() override {
                return *((double *) &value_);
            }

            virtual void *asString() override {
                return *((void **) &value_);
            }

            virtual void operator=(uint64_t value) {
                value_ = value;
            }

            virtual void operator=(int32_t value) override {
                value_ = value;
            }

            virtual void operator=(double value) override {
                value_ = *((uint64_t *) &value);
            }

            virtual void operator=(void *value) override {
                value_ = (uint64_t) value;
            }

            uint64_t *data() {
                return &value_;
            }
        };

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

        class MemDataRow : public DataRow {
        private:
            vector<MemDataField> &data_;
            uint32_t index_;
            uint8_t num_fields_;
        public:
            MemDataRow(vector<MemDataField> &data, uint8_t num_fields) : data_(data), num_fields_(num_fields) {

            }

            virtual ~MemDataRow() {

            }

            void moveto(uint32_t index) {
                index_ = index;
            }

            virtual DataField &operator[](uint64_t i) override {
                return data_[index_ * num_fields_ + i];
            }
        };


        class MemDataRowIterator : public DataRowIterator {
        private:
            MemDataRow reference;
        public:
            MemDataRowIterator(vector<MemDataField> &data, uint8_t num_fields) : reference(data, num_fields) {
            }

            virtual DataRow &operator[](uint64_t idx) override {
                reference.moveto(idx);
                return reference;
            }
        };

        class MemColumnIterator : public ColumnIterator {
        private:
            uint8_t num_fields_;
            uint8_t index_;
            vector<MemDataField> &data_;
        public:
            MemColumnIterator(vector<MemDataField> &data, uint8_t num_fields, uint8_t index) : num_fields_(num_fields),
                                                                                               index_(index),
                                                                                               data_(data) {
            }

            virtual DataField &operator[](uint64_t idx) override {
                return data_[idx * num_fields_ + index_];
            }
        };

        unique_ptr<DataRowIterator> MemBlock::rows() {
            return unique_ptr<DataRowIterator>(new MemDataRowIterator(content_, num_fields_));
        }

        unique_ptr<ColumnIterator> MemBlock::col(uint32_t col_index) {
            return unique_ptr<ColumnIterator>(new MemColumnIterator(content_, num_fields_, col_index));
        }

        ParquetBlock::ParquetBlock(shared_ptr<RowGroupReader> rowGroup, uint32_t index) : rowGroup_(rowGroup),
                                                                                          index_(index) {
        }

        ParquetBlock::~ParquetBlock() {

        }

        uint64_t ParquetBlock::size() {
            return rowGroup_->metadata()->num_rows();
        }

        unique_ptr<DataRowIterator> ParquetBlock::rows() {
            throw invalid_argument("");
        }

        class ParquetColumnIterator : public ColumnIterator, DataField {
        private:
            shared_ptr<ColumnReader> columnReader_;
            MemDataField dataField_;
            int64_t read_counter_;
        public:
            ParquetColumnIterator(shared_ptr<ColumnReader> colReader) : columnReader_(colReader), dataField_(),
                                                                        read_counter_(0) {}

            virtual ~ParquetColumnIterator() {}

            virtual DataField &operator[](uint64_t idx) override {
                columnReader_->MoveTo(idx);
                return *this;
            }

            virtual int32_t asInt() override {
                auto res = static_pointer_cast<Int32Reader>(columnReader_);
                res->ReadBatch(1, nullptr, nullptr, (int32_t *) (dataField_.data()), &read_counter_);
                return dataField_.asInt();
            }

            virtual double asDouble() override {
                auto res = static_pointer_cast<DoubleReader>(columnReader_);
                res->ReadBatch(1, nullptr, nullptr, (double *) (dataField_.data()), &read_counter_);
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


        ParquetTable::ParquetTable(const string &fileName) {
            fileReader_ = ParquetFileReader::OpenFile(fileName);
        }

        ParquetTable::~ParquetTable() {

        }

        shared_ptr<Stream<shared_ptr<Block>>> ParquetTable::blocks() {
            function<shared_ptr<Block>(const int &)> mapper = [=](const int &idx) {
                return this->createParquetBlock(idx);
            };
            auto stream = IntStream::Make(0, 10)->map(mapper);
            return stream;
        }

        shared_ptr<ParquetBlock> ParquetTable::createParquetBlock(const int &block_idx) {
            auto rowGroup = fileReader_->RowGroup(block_idx);
            return make_shared<ParquetBlock>(rowGroup, block_idx);
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

        shared_ptr<Stream<shared_ptr<Block>>> MemTable::blocks() {
            return shared_ptr<Stream<shared_ptr<Block>>>(new VectorStream<shared_ptr<Block>>(blocks_));
        }
    }
}