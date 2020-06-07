//
// Created by Harper on 6/6/20.
//

#include <gtest/gtest.h>
#include "rowcopy.h"
#include "data_model.h"

using namespace lqf;
using namespace lqf::rowcopy;

TEST(RowCopyTest, Raw) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 4, 5, 6, 7, 8};
    RowCopyFactory f;
    auto copier = f.from(I_RAW)->to(I_RAW)
            ->from_layout(col_offset)
            ->to_layout(col_offset)
            ->field(F_REGULAR, 0, 0)
            ->field(F_REGULAR, 1, 1)
            ->field(F_REGULAR, 2, 2)
            ->field(F_REGULAR, 3, 3)
            ->field(F_REGULAR, 4, 4)
            ->build();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    mdFrom[3] = 990;
    mdFrom[4] = 135;
    MemDataRow mdTo(col_offset);

    (*copier)(mdTo, mdFrom);

    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(mdFrom[i].asInt(), mdTo[i].asInt()) << i;
        mdTo[i] = 0;
    }

    RowCopyFactory f2;
    copier = f2.from(I_RAW)->to(I_RAW)
            ->from_layout(col_offset)->to_layout(col_offset)
            ->field(F_REGULAR, 0, 3)
            ->field(F_REGULAR, 1, 4)
            ->field(F_REGULAR, 2, 5)
            ->field(F_REGULAR, 3, 6)
            ->field(F_REGULAR, 4, 1)
            ->field(F_REGULAR, 5, 2)
            ->field(F_REGULAR, 6, 0)
            ->field(F_REGULAR, 7, 7)
            ->build();

    (*copier)(mdTo, mdFrom);

    EXPECT_EQ(mdFrom[0].asInt(), mdTo[3].asInt());
    EXPECT_EQ(mdFrom[1].asInt(), mdTo[4].asInt());
    EXPECT_EQ(mdFrom[2].asInt(), mdTo[5].asInt());
    EXPECT_EQ(mdFrom[3].asInt(), mdTo[6].asInt());
    EXPECT_EQ(mdFrom[4].asInt(), mdTo[1].asInt());
    EXPECT_EQ(mdFrom[5].asInt(), mdTo[2].asInt());
    EXPECT_EQ(mdFrom[6].asInt(), mdTo[0].asInt());
    EXPECT_EQ(mdFrom[7].asInt(), mdTo[7].asInt());
}

TEST(RowCopyTest, FieldCopy) {
    vector<uint32_t> col_offset{0, 1, 2, 3, 4, 5, 6, 7, 8};
    RowCopyFactory f;
    auto copier = f.from(I_OTHER)->to(I_OTHER)
            ->field(F_REGULAR, 0, 0)
            ->field(F_REGULAR, 1, 1)
            ->field(F_REGULAR, 2, 2)
            ->field(F_REGULAR, 3, 3)
            ->field(F_REGULAR, 4, 4)
            ->build();
    MemDataRow mdFrom(col_offset);
    mdFrom[0] = 135;
    mdFrom[1] = 227;
    mdFrom[2] = 881;
    mdFrom[3] = 990;
    mdFrom[4] = 135;
    MemDataRow mdTo(col_offset);

    (*copier)(mdTo, mdFrom);

    for (uint32_t i = 0; i < 5; ++i) {
        EXPECT_EQ(mdFrom[i].asInt(), mdTo[i].asInt()) << i;
    }

    for (uint32_t i = 0; i < 8; ++i) {
        mdFrom[i] = 0;
    }

    RowCopyFactory f2;
    copier = f2.from(I_RAW)->to(I_RAW)
            ->field(F_REGULAR, 0, 2)
            ->field(F_REGULAR, 1, 0)
            ->field(F_REGULAR, 2, 3)
            ->field(F_REGULAR, 3, 1)
            ->field(F_REGULAR, 4, 5)
            ->field(F_REGULAR, 5, 6)
            ->field(F_REGULAR, 6, 4)
            ->field(F_REGULAR, 7, 7)
            ->build();

    (*copier)(mdTo, mdFrom);
    EXPECT_EQ(mdFrom[0].asInt(), mdTo[2].asInt());
    EXPECT_EQ(mdFrom[1].asInt(), mdTo[0].asInt());
    EXPECT_EQ(mdFrom[2].asInt(), mdTo[3].asInt());
    EXPECT_EQ(mdFrom[3].asInt(), mdTo[1].asInt());
    EXPECT_EQ(mdFrom[4].asInt(), mdTo[5].asInt());
    EXPECT_EQ(mdFrom[5].asInt(), mdTo[6].asInt());
    EXPECT_EQ(mdFrom[6].asInt(), mdTo[4].asInt());
    EXPECT_EQ(mdFrom[7].asInt(), mdTo[7].asInt());
}