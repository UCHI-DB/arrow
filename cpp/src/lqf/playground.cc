//
// Created by Harper on 12/28/20.
//

#include <iostream>
#include <cuckoohash_map.hh>
#include <sparsehash/dense_hash_map>
#include "data_model.h"
#include "join.h"

int bsearch(int *data_, int length, int target) {
    if (data_[0] > target) {
        return -1;
    }
    if (data_[length - 1] < target) {
        return -(length + 1) - 1;
    }
    uint32_t left = 0; // item before left is less than target
    uint32_t right = length - 1; // item after right is greater than target

    while (left <= right) {
        auto middle = (left + right ) / 2;
        auto val = data_[middle];

        if (val == target) {
            return middle;
        }
        if (val > target) {
            right = middle - 1;
        } else {
            left = middle + 1;
        }
    }
    return -left - 1;
}

int main() {
    int data[1000];
    for (int i = 0; i < 1000; ++i) {
        data[i] = 2 * i;
    }
    std::cout << bsearch(data, 1000, 5)<<std::endl;
}
