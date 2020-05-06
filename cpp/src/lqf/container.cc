//
// Created by Harper on 4/20/20.
//

#include "container.h"
#include <iostream>

namespace lqf {

    namespace container {

        uint32_t ceil2(uint32_t input) {
            if (__builtin_popcount(input) == 1) {
                return input;
            }
            return 1 << (32 - __builtin_clz(input));
        }
    }
}