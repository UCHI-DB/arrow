//
// Created by harper on 2/9/20.
//

#ifndef CHIDATA_LQF_DATA_MODEL_H
#define CHIDATA_LQF_DATA_MODEL_H

#include <cstdint>
#include <parquet/file_reader.h>
#include <chidata/stream.h>

namespace chidata {
    namespace lqf {

        using namespace std;
        using namespace parquet;

        class DataField {
        public:
            virtual int32_t asInt() = 0;

            virtual double asDouble() = 0;

            virtual void *asString() = 0;

            virtual void operator=(int32_t) = 0;

            virtual void operator=(double) = 0;

            virtual void operator=(void *) = 0;
        };

        /*
         * A Union representing in-memory data fields
         */
        class DataRow {
        public:
            virtual DataField &operator[](uint64_t i) = 0;
        };

        class DataRowIterator {
        public:
            virtual DataRow &operator[](uint64_t idx) = 0;

        };

        class ColumnIterator {
        public:
            virtual DataField &operator[](uint64_t idx) = 0;
        };

        class Block {
        public:

            virtual uint64_t size() = 0;

            virtual unique_ptr<ColumnIterator> col(uint32_t col_index) = 0;

            virtual unique_ptr<DataRowIterator> rows() = 0;
        };

        class MemDataField;

        class MemBlock : public Block {
        private:
            uint32_t size_;
            uint8_t num_fields_;
            vector<MemDataField> content_;
        public:
            MemBlock(uint32_t size, uint8_t num_fields);

            virtual ~MemBlock();

            virtual uint64_t size() override;

            /**
             * Increase the block row
             * @param size
             */
            void inc(uint32_t row_to_inc);

            virtual unique_ptr<ColumnIterator> col(uint32_t col_index) override;

            virtual unique_ptr<DataRowIterator> rows() override;
        };

        class ParquetBlock : public Block {
        private:
            shared_ptr<RowGroupReader> rowGroup_;
            uint32_t index_;
        public:
            ParquetBlock(shared_ptr<RowGroupReader> rowGroup_, uint32_t index);

            virtual ~ParquetBlock();

            virtual uint64_t size() override;

            virtual unique_ptr<ColumnIterator> col(uint32_t col_index) override;

            virtual unique_ptr<DataRowIterator> rows() override;
        };

        class Table {
        public:
            virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() = 0;
        };

        class ParquetTable : public Table {
        public:
            ParquetTable(const string &fileName);

            virtual ~ParquetTable();

            virtual shared_ptr<Stream<shared_ptr<Block>>> blocks() override;

        protected:

            shared_ptr<ParquetBlock> createParquetBlock(const int &block_idx);

        private:
            unique_ptr<ParquetFileReader> fileReader_;
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
