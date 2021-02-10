//
// Created by harper on 2/9/21.
//

#ifndef LQF_STAT_H
#define LQF_STAT_H

#include <cstdint>
#include <string>
#include <unordered_map>

namespace lqf {
    namespace stat {

        class MemEstimator {
        protected:
            uint64_t size_ = 0;

            std::unordered_map<std::string, uint64_t> category_;
        public:
            static MemEstimator INST;

            MemEstimator();

            virtual ~MemEstimator() noexcept;

            void Record(std::string,uint64_t);

            void Reset();

            void Report();
        };

    }
}

#endif //LQF_STAT_H
