//
// Created by Harper on 4/20/20.
//

#include "concurrent.h"
#include <iostream>
#include <exception>

namespace lqf {

    const int32_t Int32::empty = -1;
    const int32_t Int32::min = INT32_MIN;
    const int32_t Int32::max = INT32_MAX;

    const int64_t Int64::empty = -1;
    const int64_t Int64::min = INT64_MIN;
    const int64_t Int64::max = INT64_MAX;

    namespace phasecon {

        uint32_t ceil2(uint32_t input) {
            if (__builtin_popcount(input) == 1) {
                return input;
            }
            return 1 << (32 - __builtin_clz(input));
        }
    }
}