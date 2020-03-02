//
// Created by harper on 9/14/19.
//

#ifndef SBOOST_ENCODING_RLEHYBRID_H
#define SBOOST_ENCODING_RLEHYBRID_H

#include <cstdint>

namespace sboost {
    namespace encoding {
        namespace rlehybrid {

            typedef enum {
                RLE, PACKED
            } MODE;

            void equal(const uint8_t *, uint64_t *, uint32_t, uint32_t, uint32_t, uint32_t);

            void less(const uint8_t *, uint64_t *, uint32_t, uint32_t, uint32_t, uint32_t);

            void greater(const uint8_t *, uint64_t *, uint32_t, uint32_t, uint32_t, uint32_t);

            void between(const uint8_t *, uint64_t *, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t);

            void rangele(const uint8_t *, uint64_t *, uint32_t, uint32_t, uint32_t, uint32_t, uint32_t);

        }
    }
}
#endif //SBOOST_ENCODING_RLEHYBRID_H
