//
// Created by harper on 2/6/20.
//
#include <gtest/gtest.h>
#include <vector>
#include "bitmap.h"

using namespace lqf;

TEST(FullBitmap, Cardinality) {
    auto bitmap = make_shared<FullBitmap>(1000);
    ASSERT_EQ(1000, bitmap->cardinality());
    ASSERT_TRUE(bitmap->isFull());
    ASSERT_EQ(1000, bitmap->size());
    ASSERT_EQ(1, bitmap->ratio());
}

TEST(FullBitmap, Iterator) {
    auto bitmap = make_shared<FullBitmap>(1000);
    auto ite = bitmap->iterator();

    std::vector<uint64_t> buffer = std::vector<uint64_t>();

    while (ite->hasNext()) {
        buffer.push_back(ite->next());
    }

    ASSERT_EQ(1000, buffer.size());

}

TEST(SimpleBitmap, Cardinality) {
    auto sb = make_shared<SimpleBitmap>(150000);
    sb->put(9311);
    ASSERT_EQ(1, sb->cardinality());

    sb->put(63);
    ASSERT_EQ(2, sb->cardinality());
}

TEST(SimpleBitmap, MoveTo) {
    auto sb = make_shared<SimpleBitmap>(1000);
    sb->put(241);
    sb->put(195);
    sb->put(255);
    sb->put(336);
    sb->put(855);

    auto fi = sb->iterator();
    ASSERT_EQ(195, fi->next());
    fi->moveTo(200);
    ASSERT_EQ(241, fi->next());
    fi->moveTo(336);
    ASSERT_EQ(336, fi->next());
    fi->moveTo(400);
    ASSERT_EQ(855, fi->next());

    auto sb2 = make_shared<SimpleBitmap>(150);
    sb2->put(75);
    auto fi2 = sb2->iterator();
    fi2->moveTo(76);
    ASSERT_FALSE(fi2->hasNext());
}

TEST(SimpleBitmap, Iterator) {
    auto sb = make_shared<SimpleBitmap>(1410141);
    for (int i = 1410112; i < 1410141; i++) {
        sb->put(i);
    }
    auto ite = sb->iterator();
    std::vector<uint64_t> data = std::vector<uint64_t>();
    while (ite->hasNext()) {
        uint64_t value = ite->next();
        data.push_back(value);
        ASSERT_TRUE(value < sb->size());
    }
    ASSERT_EQ(29, data.size());
    ASSERT_EQ(1410112, data[0]);
    ASSERT_EQ(1410140, data[data.size() - 1]);
}