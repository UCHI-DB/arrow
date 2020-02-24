//
// Created by harper on 2/6/20.
//

#ifndef CHIDATA_VALIDATE_H
#define CHIDATA_VALIDATE_H

#include <stdexcept>

namespace chidata {
    inline void validate_true(bool condition) {
        if (__builtin_expect(condition, 1)) {
        } else {
            throw std::invalid_argument("");
        }
    }
}
#endif //ARROW_VALIDATE_H
