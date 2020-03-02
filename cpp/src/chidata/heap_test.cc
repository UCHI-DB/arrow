//
// Created by harper on 2/27/20.
//

#include <gtest/gtest.h>
#include "heap.h"

TEST(HeapTest, Test) {
    using namespace chidata;
    Heap<int32_t *> heap(10,[]() {return new int(INT32_MAX);},
            [](int32_t *a, int32_t *b) { return *a < *b; });

    for (int i = 0; i < 100; i++) {
        heap.add(new int32_t(100 - i));
    }

    heap.done();
    auto result = heap.content();
    EXPECT_EQ(10, result.size());
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(i + 1, *result[i]);
    }

}