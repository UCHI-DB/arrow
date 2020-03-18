//
// Created by harper on 2/24/20.
//

#include <gtest/gtest.h>
#include "filter.h"
#include "data_model.h"

using namespace std;
using namespace lqf;

class ColFilterTest : public ::testing::Test {
protected:
    shared_ptr<ParquetFileReader> fileReader_;
    shared_ptr<RowGroupReader> rowGroup_;
public:
    virtual void SetUp() override {
        fileReader_ = ParquetFileReader::OpenFile("lineitem");
        rowGroup_ = fileReader_->RowGroup(0);
        return;
    }
};

TEST(RowFilterTest, Filter) {
    auto memTable = MemTable::Make(5);
    auto block1 = memTable->allocate(100);
    auto block2 = memTable->allocate(100);

    auto row1 = block1->rows();
    auto row2 = block2->rows();
    for (int i = 0; i < 100; ++i) {
        (*row1)[i][0] = i;
        (*row1)[i][1] = static_cast<double>((i - 50) * (i - 50) * 0.05);
        (*row2)[i][0] = (i + 3) * i + 1;
    }

    function<bool(DataRow &)> pred = [](DataRow &row) {
        return row[0].asInt() % 5 == 0;
    };

    auto rowFilter = RowFilter(pred);
    auto filteredTable = rowFilter.filter(*memTable);

    auto filteredBlocks = filteredTable->blocks()->collect();

    auto fblock1 = (*filteredBlocks)[0];
    auto fblock2 = (*filteredBlocks)[1];

    EXPECT_EQ(20, fblock1->size());
    EXPECT_EQ(20, fblock2->size());
}

using namespace lqf;

TEST_F(ColFilterTest, FilterOnSimpleCol) {
    auto ptable = ParquetTable::Open("lineitem", 7);
    initializer_list<ColPredicate *> list = {
            new SimpleColPredicate(0, [](const DataField &field) { return field.asInt() % 10 == 0; })};
    auto filter = make_shared<ColFilter>(list);

    auto filtered = filter->filter(*ptable);
    auto filteredblocks = filtered->blocks()->collect();

    auto table2 = ParquetTable::Open("lineitem", 7);
    auto rawblocks = table2->blocks()->collect();

    EXPECT_EQ(filteredblocks->size(), rawblocks->size());

    for (uint32_t i = 0; i < rawblocks->size(); ++i) {
        auto fb = (*filteredblocks)[i];
        auto rb = (*rawblocks)[i];

        auto bitmap = make_shared<SimpleBitmap>(rb->size());
        auto rbrows = rb->rows();
        for (uint32_t j = 0; j < rb->size(); ++j) {
            if ((*rbrows)[j][0].asInt() % 10 == 0) {
                bitmap->put(j);
            }
        }
        EXPECT_EQ(fb->size(), bitmap->cardinality());

        auto fbrows = fb->rows();
        auto bite = bitmap->iterator();

        for (uint32_t bidx = 0; bidx < bitmap->cardinality(); ++bidx) {
            fbrows->next();
            EXPECT_EQ(fbrows->pos(), bite->next());
        }
    }
}

using namespace lqf::sboost;

TEST_F(ColFilterTest, FilterSboost) {
    auto ptable = ParquetTable::Open("lineitem", (1 << 14) - 1);

    function<bool(const ByteArray &)> pred = [](const ByteArray &input) {
        return !strncmp(reinterpret_cast<const char *>(input.ptr + input.len - 3), "AIL", 3);
    };
    function<bool(const DataField &)> pred2 = [](const DataField& field) {
        ByteArray* input = field.asByteArray();
        return !strncmp(reinterpret_cast<const char *>(input->ptr + input->len - 3), "AIL", 3);
    };
    ColFilter filter({new SboostPredicate<ByteArrayType>(14, bind(&ByteArrayDictMultiEq::build, pred))});

    auto filtered = filter.filter(*ptable)->blocks()->collect();
    auto result = (*filtered)[0];

    ColFilter regFilter({new SimpleColPredicate(14, pred2)});
    auto filtered2 = regFilter.filter(*ptable)->blocks()->collect();
    auto result2 = (*filtered2)[0];

    EXPECT_EQ(result2->size(), result->size());
}