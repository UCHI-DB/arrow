//
// Created by harper on 2/3/21.
// Temporary operators that supports encoding their intermediate result
//

#ifndef LQF_ENC_OPERATOR_H
#define LQF_ENC_OPERATOR_H

#include "join.h"
#include "filter.h"
#include "tjoin.h"
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

        template<typename Container>
        class EncHashTJoin : public HashTJoin<Container> {
        protected:
            vector<parquet::Type::type> data_types_;
            vector<encoding::EncodingType> enc_types_;

            shared_ptr<Block> makeBlock(uint32_t size) override;

        public:
            EncHashTJoin(uint32_t, uint32_t, RowBuilder *,
                         initializer_list<parquet::Type::type> data_type,
                         initializer_list<encoding::EncodingType> enc_type,
                         function<bool(DataRow &, DataRow &)> pred = nullptr,
                         uint32_t expect_size = CONTAINER_SIZE);
        };

        /**
         * A predicate using SBoost to filter encoded intermediate results
         * TODO For now hard-code to support dictionary encoding
         */
        class EncMemBetweenPredicate : public ColPredicate {
        protected:
            int32_t from_;
            int32_t to_;
        public:
            EncMemBetweenPredicate(uint32_t index, int32_t from, int32_t to);

            virtual shared_ptr<Bitmap> filterBlock(Block &, Bitmap &) override;
        };

        /**
         * A filter that checks and filter bit-packed columns without delaying
         */
         class EncMatRowFilter : public Filter {

         };
    }
}

#endif //ARROW_ENC_OPERATOR_H
