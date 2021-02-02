//
// Created by harper on 2/2/21.
//
// Data Model with Encoding
//

#ifndef LQF_DATA_MODEL_ENC_H
#define LQF_DATA_MODEL_ENC_H

#include "data_model.h"

namespace lqf {

    class EncMemvColumnIterator;

    class EncMemvBlock : public Block {
    private:
        uint32_t size_;
        vector<parquet::Encoding::type> types_;
        vector<shared_ptr<Buffer>> content_;
        friend EncMemvColumnIterator;
    public:
        EncMemvBlock(initializer_list<parquet::Encoding::type>);

        virtual ~EncMemvBlock() = default;

        uint64_t size() override;

        void resize(uint32_t newsize) override;

        unique_ptr<ColumnIterator> col(uint32_t col_index) override;

        unique_ptr<DataRowIterator> rows() override;

        shared_ptr<Block> mask(shared_ptr<Bitmap> mask) override;
    };
}

#endif //ARROW_DATA_MODEL_ENC_H
