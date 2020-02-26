//
// Created by harper on 2/6/20.
//

#ifndef CHIDATA_VALIDATE_H
#define CHIDATA_VALIDATE_H

#include <stdexcept>

namespace chidata {
    inline void validate_true(bool condition, const std::string& message) {
        if (__builtin_expect(condition, 1)) {
        } else {
            throw std::invalid_argument(message);
        }
    }
}
#endif //ARROW_VALIDATE_H
