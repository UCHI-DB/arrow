//
// Created by Harper on 4/29/20.
//

#ifndef ARROW_CONCURRENT_H
#define ARROW_CONCURRENT_H

#include <cstdint>
#include <mutex>
#include <condition_variable>

namespace lqf {
    namespace concurrent {
        class Semaphore {
        private:
            std::mutex mutex_;
            std::condition_variable condition_;
            uint32_t count_ = 0; // Initialized as locked.

        public:
            void notify();

            void notify(uint32_t num);

            void wait();

            void wait(uint32_t num);

            bool try_wait();

            bool try_wait(uint32_t num);
        };
    }
}


#endif //ARROW_CONCURRENT_H
