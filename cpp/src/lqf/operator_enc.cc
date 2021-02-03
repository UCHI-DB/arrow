//
// Created by harper on 2/3/21.
//

#include "operator_enc.h"

namespace lqf {
    namespace encopr {

        EncHashJoin::EncHashJoin(uint32_t l, uint32_t r, RowBuilder *rb,
                                 initializer_list<parquet::Type::type> data_type,
                                 initializer_list<encoding::EncodingType> enc_type,
                                 function<bool(DataRow &, DataRow &)> pred,
                                 uint32_t expect_size) : HashJoin(l, r, rb, pred, expect_size), data_types_(data_type),
                                                         enc_types_(enc_type) {}

        shared_ptr<Block> EncHashJoin::makeBlock(uint32_t size) {
            return make_shared<EncMemvBlock>(data_types_, enc_types_);
        }
    }
}