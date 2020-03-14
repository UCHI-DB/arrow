//
// Created by harper on 3/14/20.
//
#include <gtest/gtest.h>
#include "executor.h"

using namespace lqf::executor;

TEST(ExecutorTest, Shutdown) {
    auto executor = Executor::Make(10);
    executor->shutdown();
}

TEST(ExecutorTest, Submit) {
    auto executor = Executor::Make(10);

    int *i = new int(0);
    auto stub = executor->submit([=]() {
        *i += 5;
    });
    stub->wait();

    EXPECT_EQ(5, *i);
    executor->shutdown();
}

TEST(ExecutorTest, InvokeAll) {

    auto executor = Executor::Make(10);

    vector<function<int32_t()>> tasks;
    for (int i = 0; i < 20; i++) {
        tasks.push_back([]() {
            usleep(1);
            auto i = time(NULL) & 0xFFFFFFFF;
            return i;
        });
    }

    unique_ptr<vector<int32_t>> result = executor->invokeAll(tasks);

    return;
}