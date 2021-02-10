//
// Created by harper on 2/2/21.
//
// Data Model with Encoding
//

#ifndef LQF_DATA_MODEL_ENC_H
#define LQF_DATA_MODEL_ENC_H

#include "data_model.h"
#include "encoding.h"


namespace lqf {


    template<typename DT>
    class EncMemvColumnIterator;

    class EncMemvBlock : public Block {
    private:
        uint32_t size_;
        vector<parquet::Type::type> data_types_;
        vector<encoding::EncodingType> encoding_types_;
        vector<shared_ptr<vector<shared_ptr<Buffer>>>> content_;

        template<typename DT>
        friend
        class EncMemvColumnIterator;

    public:
        EncMemvBlock(vector<parquet::Type::type>, vector<encoding::EncodingType>);

        EncMemvBlock(initializer_list<parquet::Type::type>, initializer_list<encoding::EncodingType>);

        EncMemvBlock(EncMemvBlock&);

        virtual ~EncMemvBlock() = default;

        uint64_t size() override;

        void resize(uint32_t newsize) override;

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;

        inline shared_ptr<vector<shared_ptr<Buffer>>> rawcol(uint32_t index) { return content_[index]; }

        uint64_t memrss() override;
    };
}

#endif //ARROW_DATA_MODEL_ENC_H
