//
// Created by Harper on 4/20/20.
//

#include <gtest/gtest.h>
#include <functional>
#include <unordered_set>
#include <unordered_map>
#include "concurrent.h"
#include "executor.h"

using namespace lqf::executor;
using namespace lqf;
using namespace lqf::phasecon;

TEST(PhaseConcurrentHashSetTest, Insert) {
    PhaseConcurrentHashSet<Int64> hashSet;

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_set<int> serial;
    vector<int32_t> values;

    srand(time(NULL));

    int total = 1000000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        values.push_back(((uint32_t) rand()) % 1000);
        serial.insert(values.back());
    }
    function<int(int)> task = [&hashSet, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            hashSet.add(values[input * sliver + i]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }

    executor->invokeAll(tasks);


    EXPECT_EQ(hashSet.size(), serial.size());
    for (auto &val :serial) {
        EXPECT_TRUE(hashSet.test(val)) << val;
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_FALSE(hashSet.test(i + 1000)) << (i + 1000);
    }
}

TEST(PhaseConcurrentHashSetTest, Resize) {
    PhaseConcurrentHashSet<Int32> hashSet(2000);

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_set<int> serial;
    vector<int32_t> values;

    srand(time(NULL));

    int total = 10000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        values.push_back(rand() % 1000);
        serial.insert(values.back());
    }
    function<int(int)> task = [&hashSet, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            hashSet.add(values[input * sliver + i]);
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }

    executor->invokeAll(tasks);

    EXPECT_EQ(hashSet.size(), serial.size());
    for (auto &val :serial) {
        EXPECT_TRUE(hashSet.test(val)) << val;
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_FALSE(hashSet.test(i + 1000)) << (i + 1000);
    }

    hashSet.resize(3000);

    EXPECT_EQ(hashSet.limit(), 4096);

    EXPECT_EQ(hashSet.size(), serial.size());
    for (auto &val :serial) {
        EXPECT_TRUE(hashSet.test(val)) << val;
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_FALSE(hashSet.test(i + 1000)) << (i + 1000);
    }
}

TEST(PhaseConcurrentHashMapTest, Insert) {
    struct DemoObject {
        int value1;
        int value2;
    };
    PhaseConcurrentHashMap<Int32, DemoObject *> hashMap;

    auto executor = Executor::Make(10);

    vector<function<int()>> tasks;

    unordered_map<int, DemoObject *> serial;
    vector<int32_t> keys;
    vector<unique_ptr<DemoObject>> values;

    srand(time(NULL));

    int total = 10000;
    int sliver = total / 10;

    for (int i = 0; i < total; ++i) {
        int key = i;
        DemoObject *value = new DemoObject{rand(), rand()};

        keys.push_back(key);
        values.push_back(unique_ptr<DemoObject>(value));

        serial[key] = value;
    }
    function<int(int)> task = [&hashMap, &keys, &values, sliver](int input) {
        for (int i = 0; i < sliver; ++i) {
            auto index = input * sliver + i;
            hashMap.put(keys[index], values[index].get());
        }
        return input;
    };
    for (int i = 0; i < 10; ++i) {
        tasks.push_back(bind(task, i));
    }
    executor->invokeAll(tasks);


    EXPECT_EQ(hashMap.size(), serial.size());
    for (auto &entry:serial) {
        EXPECT_EQ(hashMap.get(entry.first), entry.second);
    }
}

TEST(PhaseConcurrentHashMapTest, Delete) {

}