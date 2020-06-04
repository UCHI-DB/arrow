//
// Created by Harper on 6/4/20.
//

#ifndef LQF_MEMORY_H
#define LQF_MEMORY_H

#include <cstdint>
#include <memory>

namespace lqf {
    namespace memory {

        /**
         * Store the ByteArray data used in query
         */
        class ByteArrayBuffer {

        public:
            ByteArrayBuffer();

            virtual ~ByteArrayBuffer();

            uint8_t*
        };

    }
}


#endif //LQF_MEMORY_H
