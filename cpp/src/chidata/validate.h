//
// Created by harper on 2/6/20.
//

#ifndef CHIDATA_VALIDATE_H
#define CHIDATA_VALIDATE_H

#include <stdexcept>

namespace chidata {
    namespace util {

        inline void validate_true(bool condition) {
            if (!condition) {
                throw std::invalid_argument("");
            }
        }
    }
}
#endif //ARROW_VALIDATE_H
