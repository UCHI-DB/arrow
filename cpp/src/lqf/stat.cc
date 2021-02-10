//
// Created by harper on 2/9/21.
//

#include "stat.h"
#include <iostream>

namespace lqf {
    namespace stat {

        MemEstimator MemEstimator::INST = MemEstimator();

        MemEstimator::MemEstimator() {}

        MemEstimator::~MemEstimator() noexcept {}

        void MemEstimator::Record(std::string category, uint64_t size) {
            size_ += size;
            auto found = category_.find(category);
            if (found == category_.end()) {
                category_[category] = size;
            } else {
                found->second += size;
            }
        }

        void MemEstimator::Reset() {
            size_ = 0;
            category_.clear();
        }

        void MemEstimator::Report() {
            std::cout << "Mem Estimator: " << size_ << '\n';

            for(auto& entry: category_) {
                std::cout << "Mem in Category: " << entry.first << " = " << entry.second << '\n';
            }
        }
    }
}