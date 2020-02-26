//
// Created by harper on 2/13/20.
//

#include <vector>
#include <gtest/gtest.h>
#include "stream.h"

using namespace chidata;
using namespace std;

TEST(IntStream, CreateStream) {
    auto stream = IntStream::Make(1, 10);
    vector<int32_t> buffer;

    while (!stream->isEmpty()) {
        buffer.push_back(stream->next());
    }
    ASSERT_EQ(9, buffer.size());
    for (int i = 1; i < 10; i++) {
        ASSERT_EQ(i, buffer[i - 1]);
    }
}

class TestClass {
protected:
    int32_t value_;
public:
    static int32_t counter;

    TestClass(int32_t value) : value_(value) {}

    virtual ~TestClass() {
        counter++;
    }

    int32_t GetValue() {
        return value_;
    }
};

int32_t TestClass::counter = 0;

TEST(Stream, Map) {
    TestClass::counter = 0;

    auto stream = IntStream::Make(1, 10);

    function<unique_ptr<TestClass>(const int32_t &)> func = [](const int32_t &val) {
        return unique_ptr<TestClass>(new TestClass(val));
    };

    auto mapped = stream->map(func);

    auto buffer = mapped->collect();

    ASSERT_EQ(9, buffer->size());

    for (int i = 0; i < 9; ++i) {
        ASSERT_EQ(i + 1, (*buffer)[i]->GetValue());
    }

    ASSERT_EQ(0, TestClass::counter);

    buffer->clear();

    ASSERT_EQ(9, TestClass::counter);
}

