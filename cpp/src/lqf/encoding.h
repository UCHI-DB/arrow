//
// Created by harper on 2/2/21.
//
// Contains encoding wrapper for intermediate result
// Due to time reason, only support integer type for now.
//

#include <cstdint>
#include <memory>
#include <vector>
#include <immintrin.h>
#include <arrow/buffer.h>
#include <parquet/types.h>

#ifndef LQF_ENCODING_H
#define LQF_ENCODING_H

namespace lqf {
    namespace encoding {

        using namespace std;
        using namespace arrow;

        enum EncodingType {
            PLAIN, DICTIONARY, BITPACK
        };

        // Instead of having a template for each Encoder/Decoder
        // we choose to just overload the method.

        template<typename DT>
        class Encoder {
            using dtype = typename DT::c_type;
        public:
            virtual void Add(dtype) = 0;

            virtual shared_ptr<vector<shared_ptr<Buffer>>> Dump() = 0;
        };

        template<typename DT>
        class Decoder {
            using dtype = typename DT::c_type;
        public:
            virtual void SetData(shared_ptr<vector<shared_ptr<Buffer>>> data) = 0;

            virtual uint32_t Decode(dtype *dest, uint32_t expect) = 0;
        };

        template<typename DT>
        unique_ptr<Encoder<DT>> GetEncoder(EncodingType type);

        template<>
        unique_ptr<Encoder<parquet::Int32Type>> GetEncoder<parquet::Int32Type>(EncodingType type);

        template<typename DT>
        unique_ptr<Decoder<DT>> GetDecoder(EncodingType type);

        template<>
        unique_ptr<Decoder<parquet::Int32Type>> GetDecoder<parquet::Int32Type>(EncodingType type);
    }
}

#endif //ARROW_ENCODING_H
