//
// Created by harper on 2/6/20.
//
#include <gtest/gtest.h>
#include <vector>
#include "bitmap.h"
#include "threadpool.h"

using namespace lqf;

class RleBitmapForTest : public RleBitmap {
public:
    RleBitmapForTest(uint64_t size) : RleBitmap(size) {}

    uint32_t search(uint64_t pos) {
        return _search(pos);
    }
};

TEST(RleBitmap, Search) {
    RleBitmapForTest bitmap(1000);

    for (int i = 0; i < 1000; ++i) {
        if (i % 2) {
            bitmap.put(i);
        }
    }

    for (int i = 0; i < 1000; ++i) {
        auto res = bitmap.search(i);
        if (i % 2) {
            ASSERT_EQ(res, i / 2);
        } else {
            ASSERT_EQ(-res - 1, i / 2);
        }
    }

}

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

TEST(SimpleBitmapTest, Cardinality) {
    auto sb = make_shared<SimpleBitmap>(150000);
    sb->put(9311);
    ASSERT_EQ(1, sb->cardinality());

    sb->put(63);
    ASSERT_EQ(2, sb->cardinality());
}

TEST(SimpleBitmapTest, Negate) {
    auto sb1 = make_shared<SimpleBitmap>(64);
    sb1->put(37);
    ASSERT_EQ(1, sb1->cardinality());
    ~(*sb1);
    ASSERT_EQ(63, sb1->cardinality());

    auto sb2 = make_shared<SimpleBitmap>(100);
    sb2->put(37);
    ASSERT_EQ(1, sb2->cardinality());
    ~(*sb2);
    ASSERT_EQ(99, sb2->cardinality());
}

TEST(SimpleBitmapTest, MoveTo) {
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

TEST(SimpleBitmapTest, Iterator) {
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

TEST(SimpleBitmapTest, InvIterator) {
    auto sb = make_shared<SimpleBitmap>(10000);
    for (int i = 0; i < 10000; i++) {
        if (i % 3 == 0)
            sb->put(i);
    }
    auto ite = sb->inv_iterator();
    std::vector<uint64_t> data = std::vector<uint64_t>();
    while (ite->hasNext()) {
        uint64_t value = ite->next();
        data.push_back(value);
        ASSERT_TRUE(value < sb->size()) << value;
    }
    ASSERT_EQ(6666, data.size());
    ASSERT_EQ(1, data[0]);

    for (int i = 0; i < 6666; ++i) {
        ASSERT_EQ(data[i], 3 * (i / 2) + i % 2 + 1) << i;
    }
    ASSERT_EQ(9998, data[data.size() - 1]);
}

TEST(SimpleBitmapTest, Mask) {
    auto big = make_shared<SimpleBitmap>(1000);
    for (int i = 0; i < 1000; ++i) {
        if (i % 5 && !((i + 1) % 5 == 0 && i % 2 == 0)) {
            big->put(i);
        }
    }
    EXPECT_EQ(700, big->cardinality());
    auto small = make_shared<SimpleBitmap>(700);
    for (int i = 0; i < 700; ++i) {
        if (i % 3 == 0) {
            small->put(i);
        }
    }
    auto masked = big->mask(*small);

    vector<int32_t> data1;
    vector<int32_t> data;
    for (int i = 0; i < 1000; ++i) {
        if (i % 5 && !((i + 1) % 5 == 0 && i % 2 == 0)) {
            data1.push_back(i);
        }
    }
    for (uint32_t i = 0; i < data1.size(); ++i) {
        if (i % 3 == 0)
            data.push_back(data1[i]);
    }

    EXPECT_EQ(1000, masked->size());
    EXPECT_EQ(234, masked->cardinality());

    auto ite = masked->iterator();
    for (uint32_t i = 0; i < 234; ++i) {
        EXPECT_EQ(data[i], ite->next());
    }
}

TEST(SimpleBitmapTest, BitwiseAnd) {

    auto bitmap1 = make_shared<SimpleBitmap>(1410541);
    auto bitmap2 = make_shared<SimpleBitmap>(1410541);

    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 2 == 0)
            bitmap1->put(i);
        if (i % 5 == 0)
            bitmap2->put(i);
    }

    auto andres = (*bitmap1) & (*bitmap2);

    EXPECT_EQ(andres->size(), 1410541);
    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 10 == 0)
            EXPECT_TRUE(andres->check(i)) << i;
        else
            EXPECT_FALSE(andres->check(i)) << i;
    }
}

TEST(SimpleBitmapTest, BitwiseOr) {

    auto bitmap1 = make_shared<SimpleBitmap>(1410541);
    auto bitmap2 = make_shared<SimpleBitmap>(1410541);

    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 7 == 0)
            bitmap1->put(i);
        if (i % 5 == 0)
            bitmap2->put(i);
    }

    auto res = (*bitmap1) | (*bitmap2);

    EXPECT_EQ(res->size(), 1410541);
    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 7 == 0 || i % 5 == 0)
            EXPECT_TRUE(res->check(i)) << i;
        else
            EXPECT_FALSE(res->check(i)) << i;
    }
}

TEST(SimpleBitmapTest, BitwiseXor) {

    auto bitmap1 = make_shared<SimpleBitmap>(1410541);
    auto bitmap2 = make_shared<SimpleBitmap>(1410541);

    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 7 == 0)
            bitmap1->put(i);
        if (i % 5 == 0)
            bitmap2->put(i);
    }

    auto andres = (*bitmap1) ^(*bitmap2);

    EXPECT_EQ(andres->size(), 1410541);
    for (uint32_t i = 0; i < 1410541; ++i) {
        if ((i % 7 == 0 || i % 5 == 0) && i % 35 != 0)
            EXPECT_TRUE(andres->check(i)) << i;
        else
            EXPECT_FALSE(andres->check(i)) << i;
    }
}

TEST(SimpleBitmapTest, BitwiseNot) {

    auto bitmap1 = make_shared<SimpleBitmap>(1410541);

    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 7 == 0)
            bitmap1->put(i);
    }

    auto andres = ~(*bitmap1);

    EXPECT_EQ(andres->size(), 1410541);
    for (uint32_t i = 0; i < 1410541; ++i) {
        if (i % 7)
            EXPECT_TRUE(andres->check(i)) << i;
        else
            EXPECT_FALSE(andres->check(i)) << i;
    }
}

using namespace threadpool;

TEST(ConcurrentBitmapTest, Put) {
    ConcurrentBitmap conc(1525025);
    auto executor = Executor::Make(30);

    function<int(int)> task = [&conc](int index) {
        for (int i = 0; i < 23828; ++i) {
            conc.put(i * 64 + index);
        }
        return 0;
    };
    vector<int32_t> params{0, 3, 5, 10, 9, 8, 2, 7, 6, 11, 22, 35, 47, 16, 21};

    vector<function<int()>> tasks;
    for (uint32_t i = 0; i < params.size(); ++i) {
        tasks.push_back(bind(task, params[i]));
    }

    executor->invokeAll(tasks);

    auto raw = conc.raw();
    auto sum = 0L;
    for (uint32_t i = 0; i < params.size(); ++i) {
        sum |= 1L << params[i];
    }
    for (uint32_t i = 0; i < 23828; ++i) {
        EXPECT_EQ(raw[i], sum);
    }
}