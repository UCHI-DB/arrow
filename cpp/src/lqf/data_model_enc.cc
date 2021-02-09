//
// Created by harper on 2/2/21.
//

#include "data_model_enc.h"
#include <type_traits>

namespace lqf {

    EncMemvBlock::EncMemvBlock(initializer_list<parquet::Type::type> data_type,
                               initializer_list<encoding::EncodingType> enc_type)
            : size_(0), data_types_(data_type), encoding_types_(enc_type) {}

    EncMemvBlock::EncMemvBlock(vector<parquet::Type::type> data_type, vector<encoding::EncodingType> enc_type)
            : size_(0), data_types_(data_type), encoding_types_(enc_type) {}

    uint64_t EncMemvBlock::size() {
        return size_;
    }

    void EncMemvBlock::resize(uint32_t newsize) {
        size_ = newsize;
    }

    template<typename DT>
    class EncMemvColumnIterator : public ColumnIterator {
        using data_type = typename DT::c_type;
    private:
        EncMemvBlock &block_;
        uint32_t col_index_;

        uint64_t row_index_;
        DataField view_;
        data_type buffer_[10];

        uint32_t buffer_start_ = 0;
        uint32_t buffer_end_ = 0;

        bool read_ = false;
        encoding::EncodingType type_;
        unique_ptr<encoding::Encoder<DT>> encoder_;
        unique_ptr<encoding::Decoder<DT>> decoder_;

        void read_buffer() {
            while (buffer_end_ <= row_index_) {
                auto decoded = decoder_->Decode(buffer_, 10);
                buffer_start_ = buffer_end_;
                buffer_end_ += decoded;
            }
            view_ = (uint64_t *) (buffer_ + row_index_ - buffer_start_);
        }

        void write_buffer() {
            encoder_->Add(buffer_[0]);
        }

    public:
        EncMemvColumnIterator(EncMemvBlock &block, uint32_t col_index)
                : block_(block), col_index_(col_index), row_index_(-1), type_(block.encoding_types_[col_index]) {
            view_.size_ = 1;
            view_ = (uint64_t *) buffer_;
            read_ = block.size_ > 0;
            if (read_) {
                // read mode
                decoder_ = encoding::GetDecoder<DT>(type_);
                auto buffer = block_.content_[col_index_];
                decoder_->SetData(buffer);
            } else {
                // write mode
                encoder_ = encoding::GetEncoder<DT>(type_);
            }
        }

        DataField &operator[](uint64_t idx) override {
            auto old = row_index_;
            row_index_ = idx;
            if (!read_) {
                if (block_.size_ < row_index_ + 1)
                    block_.size_ = row_index_ + 1;
                if (old != (uint64_t) -1)
                    write_buffer();
            } else {
                read_buffer();
            }
            return view_;
        }

        DataField &next() override {
            ++row_index_;
            if (!read_) {
                if (block_.size_ < row_index_ + 1)
                    block_.size_ = row_index_ + 1;
                if (row_index_ > 0)
                    write_buffer();
            } else {
                read_buffer();
            }
            return view_;
        }

        uint64_t pos() override {
            return row_index_;
        }

        void close() override {
            if (!read_) {
                // Write buffer
                write_buffer();
                while (block_.content_.size() <= col_index_) {
                    block_.content_.push_back(nullptr);
                }
                block_.content_[col_index_] = encoder_->Dump();
            }
        }
    };

    class EncMemvDataRowIterator;

    class EncMemvDataRowView : public DataRow {
    private:
        uint64_t index_;

        vector<unique_ptr<ColumnIterator>> &cols_;

        friend EncMemvDataRowIterator;
    public:
        EncMemvDataRowView(vector<unique_ptr<ColumnIterator>> &cols)
                : index_(-1), cols_(cols) {
        }

        virtual ~EncMemvDataRowView() {}

        void moveto(uint64_t index) {
            index_ = index;
        }

        void next() {
            ++index_;
        }

        DataField &operator[](uint64_t i) override {
            return (*cols_[i])[index_];
        }

        uint32_t num_fields() override {
            return cols_.size();
        }

        DataRow &operator=(DataRow &row) override {
            uint32_t num_cols = cols_.size();
            for (uint32_t i = 0; i < num_cols; ++i) {
                (*this)[i] = row[i];
            }
            return *this;
        }

        unique_ptr<DataRow> snapshot() override {
            MemDataRow *copy = new MemDataRow(cols_.size());
            (*copy) = (*this);
            return unique_ptr<DataRow>(copy);
        }
    };

    class EncMemvDataRowIterator : public DataRowIterator {
    private:
        vector<unique_ptr<ColumnIterator>> cols_;
        EncMemvDataRowView reference_;
    public:
        EncMemvDataRowIterator(vector<unique_ptr<ColumnIterator>> &cols) : cols_(move(cols)), reference_(cols_) {}

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

        void close() override {
            for (auto &col:cols_) {
                col->close();
            }
        }
    };

    unique_ptr<DataRowIterator> EncMemvBlock::rows() {
        vector<unique_ptr<ColumnIterator>> cols;
        for (uint32_t i = 0; i < data_types_.size(); ++i) {
            auto coli = col(i);
            cols.push_back(move(coli));
        }
        return unique_ptr<DataRowIterator>(new EncMemvDataRowIterator(cols));
    }

    unique_ptr<ColumnIterator> EncMemvBlock::col(uint32_t col_index) {
        auto type = data_types_[col_index];
        switch (type) {
            case parquet::Type::type::INT32:
                return unique_ptr<ColumnIterator>(new EncMemvColumnIterator<parquet::Int32Type>(*this, col_index));
            case parquet::Type::type::DOUBLE:
                return unique_ptr<ColumnIterator>(new EncMemvColumnIterator<parquet::DoubleType>(*this, col_index));
            default:
                return nullptr;
        }
    }

    shared_ptr<Block> EncMemvBlock::mask(shared_ptr<Bitmap> mask) {
        // Does not support
        return make_shared<MaskedBlock>(shared_from_this(), mask);
    }

}