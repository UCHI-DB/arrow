//
// Created by harper on 2/6/20.
//
#include <gtest/gtest.h>
#include <vector>
#include "bitmap.h"

TEST(FullBitmap, Cardinality) {
    using namespace chidata;
    FullBitmap *bitmap = new FullBitmap(1000);
    ASSERT_EQ(1000, bitmap->cardinality());
    ASSERT_TRUE(bitmap->is_full());
    ASSERT_EQ(1000, bitmap->size());
    ASSERT_EQ(1, bitmap->ratio());

    delete bitmap;
}

TEST(FullBitmap, Iterator) {
    using namespace chidata;
    FullBitmap *bitmap = new FullBitmap(1000);
    BitmapIterator *ite = bitmap->iterator().get();

    std::vector<uint64_t> buffer = std::vector<uint64_t>();

    while(ite->has_next()) {
        buffer.push_back(ite->next());
    }

    ASSERT_EQ(1000,buffer.size());

    delete ite;
    delete bitmap;
}

TEST(SimpleBitmap, Cardinality) {
    using namespace chidata;
    SimpleBitmap *sb = new SimpleBitmap(150000);
    sb->put(9311);
    ASSERT_EQ(1, sb->cardinality());

    sb->put(63);
    ASSERT_EQ(2, sb->cardinality());
    delete sb;
}

TEST(SimpleBitmap, MoveTo) {
    using namespace chidata;
    SimpleBitmap *sb = new SimpleBitmap(1000);
    sb->put(241);
    sb->put(195);
    sb->put(255);
    sb->put(336);
    sb->put(855);

    BitmapIterator *fi = sb->iterator().get();
    ASSERT_EQ(195, fi->next());
    fi->move_to(200);
    ASSERT_EQ(241, fi->next());
    fi->move_to(336);
    ASSERT_EQ(336, fi->next());
    fi->move_to(400);
    ASSERT_EQ(855, fi->next());

    delete fi;
    delete sb;

    SimpleBitmap *sb2 = new SimpleBitmap(150);
    sb2->put(75);
    BitmapIterator *fi2 = sb2->iterator().get();
    fi2->move_to(76);
    ASSERT_FALSE(fi2->has_next());
    delete fi2;
    delete sb2;
}

TEST(SimpleBitmap, Iterator) {
    using namespace chidata;
    SimpleBitmap *sb = new SimpleBitmap(1410141);
    for (int i = 1410112; i < 1410141; i++) {
        sb->put(i);
    }
    BitmapIterator *ite = sb->iterator().get();
    std::vector<uint64_t> data = std::vector<uint64_t>();
    while (ite->has_next()) {
        uint64_t value = ite->next();
        data.push_back(value);
        ASSERT_TRUE(value < sb->size());
    }
    ASSERT_EQ(29, data.size());
    ASSERT_EQ(1410112, data[0]);
    ASSERT_EQ(1410140, data[data.size() - 1]);

    delete ite;
    delete sb;
}