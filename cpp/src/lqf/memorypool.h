//
// Created by Harper on 6/4/20.
//

#ifndef LQF_MEMORY_H
#define LQF_MEMORY_H

#include <cstdint>
#include <memory>
#include <vector>
#include <mutex>
#include <parquet/types.h>

#define SLAB_SIZE 1048576

namespace lqf {
    namespace memory {

        using namespace std;

        /**
         * Store the ByteArray data used in query
         */
        class ByteArrayBuffer {
        protected:
            static thread_local uint32_t index_;
            static thread_local uint32_t offset_;

            mutex assign_lock_;
            vector<uint8_t *> buffer_;

            void new_slab();

        public:
            ByteArrayBuffer();

            virtual ~ByteArrayBuffer();

            void allocate(parquet::ByteArray &input);

            static ByteArrayBuffer instance;
        };

    }
}


#endif //LQF_MEMORY_H
