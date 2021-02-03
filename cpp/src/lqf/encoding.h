//
// Created by harper on 2/2/21.
//
// Contains encoding wrapper for intermediate result
// Due to time reason, only support integer type for now.
//

#include <cstdint>
#include <memory>
#include <vector>
#include <arrow/buffer.h>

#ifndef LQF_ENCODING_H
#define LQF_ENCODING_H

namespace lqf {
    namespace encoding {

        using namespace std;
        using namespace arrow;

        class Encoder {
        public:
            virtual void Add(int32_t value) = 0;

            virtual shared_ptr<vector<shared_ptr<Buffer>>> Dump() = 0;
        };

        class Decoder {
        public:
            virtual void SetData(shared_ptr<vector<shared_ptr<Buffer>>> data) = 0;

            virtual uint32_t Decode(int32_t *dest, uint32_t expect) = 0;
        };

        enum Type {
            DICTIONARY
        };

        unique_ptr<Encoder> GetEncoder(Type type);

        unique_ptr<Decoder> GetDecoder(Type type);
    }
}

#endif //ARROW_ENCODING_H
