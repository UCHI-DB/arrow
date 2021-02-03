//
// Created by harper on 2/3/21.
// Temporary operators that supports encoding their intermediate result
//

#ifndef LQF_ENC_OPERATOR_H
#define LQF_ENC_OPERATOR_H

#include "join.h"
#include "encoding.h"
#include "data_model_enc.h"

namespace lqf {
    namespace encopr {

        class EncHashJoin : public HashJoin {
        protected:
            vector<parquet::Type::type> data_types_;
            vector<encoding::EncodingType> enc_types_;

            shared_ptr<Block> makeBlock(uint32_t size) override;

        public:
            EncHashJoin(uint32_t, uint32_t, RowBuilder *,
                        initializer_list<parquet::Type::type> data_type,
                        initializer_list<encoding::EncodingType> enc_type,
                        function<bool(DataRow &, DataRow &)> pred = nullptr,
                        uint32_t expect_size = CONTAINER_SIZE);
        };
    }
}

#endif //ARROW_ENC_OPERATOR_H
