//
// Created by Harper on 6/16/20.
//

#include <gtest/gtest.h>
#include "data_container.h"

using namespace lqf;
using namespace lqf::datacontainer;

TEST(MemRowVectorTest, Pushback) {
    MemRowVector vector(colOffset(3));
    for (auto i = 0; i < 500000; ++i) {
        DataRow &writer = vector.push_back();
        EXPECT_EQ(writer.size(), 3);
        writer[0] = i;
        writer[1] = i * 0.1;
        writer[2] = i * 2;
    }
    EXPECT_EQ(500000, vector.size());
}

TEST(MemRowVectorTest, Access) {
    MemRowVector vector(colOffset(3));
    for (auto i = 0; i < 500000; ++i) {
        DataRow &writer = vector.push_back();
        EXPECT_EQ(writer.size(), 3);
        writer[0] = i;
        writer[1] = i * 0.1;
        writer[2] = i * 2;
    }
    EXPECT_EQ(500000, vector.size());

    for (auto i = 0; i < 500000; ++i) {
        DataRow &row = vector[i];
        ASSERT_EQ(i, row[0].asInt()) << i;
        ASSERT_EQ(i * 0.1, row[1].asDouble()) << i;
        ASSERT_EQ(i * 2, row[2].asInt()) << i;
    }
}

TEST(MemRowVectorTest, Iterator) {
    MemRowVector vector(colOffset(3));
    for (auto i = 0; i < 500000; ++i) {
        DataRow &writer = vector.push_back();
        EXPECT_EQ(writer.size(), 3);
        writer[0] = i;
        writer[1] = i * 0.1;
        writer[2] = i * 2;
    }
    EXPECT_EQ(500000, vector.size());

    auto iterator = vector.iterator();
    auto counter = 0;
    while (iterator->hasNext()) {
        DataRow &row = iterator->next();
        ASSERT_EQ(counter, row[0].asInt()) << counter;
        ASSERT_EQ(counter * 0.1, row[1].asDouble()) << counter;
        ASSERT_EQ(counter * 2, row[2].asInt()) << counter;
        counter++;
    }
    EXPECT_EQ(counter, 500000);
}

TEST(MemRowMapTest, Insert) {
    MemRowMap map(colOffset(3));
    for (int i = 0; i < 500000; ++i) {
        DataRow &row = map.insert(3 * i + 42);
        row[0] = i;
        row[1] = i * 2 + 0.1;
        row[2] = 3 * i;
    }
    EXPECT_EQ(map.size(), 500000);
}


TEST(MemRowMapTest, Access) {
    MemRowMap map(colOffset(3));
    for (int i = 0; i < 500000; ++i) {
        DataRow &row = map.insert(3 * i + 42);
        row[0] = i;
        row[1] = i * 2 + 0.1;
        row[2] = 3 * i;
    }
    EXPECT_EQ(map.size(), 500000);

    for (int i = 0; i < 500000; ++i) {
        DataRow *row = map.find(3 * i + 42);
        ASSERT_EQ((*row)[0].asInt(), i);
        ASSERT_EQ((*row)[1].asDouble(), i * 2 + 0.1);
        ASSERT_EQ((*row)[2].asInt(), i * 3);
    }
    for (int i = 0; i < 1000; ++i) {
        DataRow *row = map.find(3 * (i + 500000) + 42);
        EXPECT_EQ(nullptr, row);
    }
}

TEST(MemRowMapTest, Iterator) {
    MemRowMap map(colOffset(3));
    for (auto i = 0; i < 500000; ++i) {
        DataRow &writer = map.insert(3 * i + 42);
        EXPECT_EQ(writer.size(), 3);
        writer[0] = i;
        writer[1] = i * 0.1;
        writer[2] = i * 2;
    }
    EXPECT_EQ(500000, map.size());

    set<int> collection;
    auto iterator = map.iterator();
    auto counter = 0;
    while (iterator->hasNext()) {
        auto &row = iterator->next();
        auto value = row[0].asInt();
        ASSERT_EQ(value * 0.1, row[1].asDouble()) << counter;
        ASSERT_EQ(value * 2, row[2].asInt()) << counter;
        collection.insert(value);
        counter++;
    }
    EXPECT_EQ(counter, 500000);
    EXPECT_EQ(collection.size(), 500000);
}

TEST(MemRowMapTest, MapIterator) {
    MemRowMap map(colOffset(3));
    for (auto i = 0; i < 500000; ++i) {
        DataRow &writer = map.insert(3 * i + 42);
        EXPECT_EQ(writer.size(), 3);
        writer[0] = i;
        writer[1] = i * 0.1;
        writer[2] = i * 2;
    }
    EXPECT_EQ(500000, map.size());

    set<int> collection;
    auto iterator = map.map_iterator();
    auto counter = 0;
    while (iterator->hasNext()) {
        auto &row = iterator->next();
        auto value = row.second[0].asInt();
        ASSERT_EQ(row.first, value * 3 + 42);
        ASSERT_EQ(value * 0.1, row.second[1].asDouble()) << counter;
        ASSERT_EQ(value * 2, row.second[2].asInt()) << counter;
        collection.insert(value);
        counter++;
    }
    EXPECT_EQ(counter, 500000);
    EXPECT_EQ(collection.size(), 500000);
}


TEST(ConcurrentMemRowMapTest, Insert) {
    ConcurrentMemRowMap map(1048576, colOffset(3));
    for (int i = 0; i < 500000; ++i) {
        DataRow &row = map.insert(i + 42824);
        row[0] = i;
        row[1] = i * 2 + 0.1;
        row[2] = 3 * i;
    }
    EXPECT_EQ(map.size(), 500000);
}

TEST(ConcurrentMemRowMapTest, InsertMultiThread) {
    auto executor = Executor::Make(10);
    ConcurrentMemRowMap map(1048576, colOffset(3));
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back([i, &map]() {
            for (int j = 0; j < 50000; ++j) {
                int key = i * 50000 + j;
                DataRow &row = map.insert(key);
                row[0] = key;
                row[1] = key * 2 + 0.1;
                row[2] = key * 3;
            }
            return 0;
        });
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(map.size(), 500000);
}

TEST(ConcurrentMemRowMapTest, Access) {
    ConcurrentMemRowMap map(1048576, colOffset(3));
    auto executor = Executor::Make(10);
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back([i, &map]() {
            for (int j = 0; j < 50000; ++j) {
                int key = i * 50000 + j;
                DataRow &row = map.insert(key);
                row[0] = key;
                row[1] = key * 2 + 0.1;
                row[2] = key * 3;
            }
            return 0;
        });
    }
    executor->invokeAll(tasks);

    EXPECT_EQ(map.size(), 500000);

    for (int i = 0; i < 500000; ++i) {
        DataRow *row = map.find(i);
        EXPECT_EQ((*row)[0].asInt(), i);
        EXPECT_EQ((*row)[1].asDouble(), i * 2 + 0.1);
        EXPECT_EQ((*row)[2].asInt(), i * 3);
    }
    for (int i = 0; i < 1000; ++i) {
        DataRow *row = map.find(i + 500000);
        EXPECT_EQ(nullptr, row);
    }
}

TEST(ConcurrentMemRowMapTest, AccessMultiThread) {
    ConcurrentMemRowMap map(1048576, colOffset(3));
    for (int i = 0; i < 500000; ++i) {
        DataRow &row = map.insert(i + 42824);
        row[0] = i;
        row[1] = i * 2 + 0.1;
        row[2] = 3 * i;
    }
    EXPECT_EQ(map.size(), 500000);

    auto executor = Executor::Make(10);
    vector<function<int()>> tasks;
    for (int i = 0; i < 10; ++i) {
        tasks.push_back([i, &map]() {
            for (int j = 0; j < 50000; ++j) {
                int key = i * 50000 + j;
                int value = key + 42824;
                DataRow *row = map.find(value);
                EXPECT_EQ((*row)[0].asInt(), key);
                EXPECT_EQ((*row)[1].asDouble(), key * 2 + 0.1);
                EXPECT_EQ((*row)[2].asInt(), key * 3);
            }
            return 0;
        });
    }
    executor->invokeAll(tasks);
}
