//
// Created by harper on 2/14/20.
//

#include <gtest/gtest.h>
#include "data_model.cc"

using namespace lqf;


TEST(MemBlockTest, Column) {
    MemBlock mb(100, 5);

    auto col = mb.col(0);

    (*col)[4] = 3.27f;

    ASSERT_DOUBLE_EQ(3.27f, (*col)[4].asDouble());
}

TEST(MemBlockTest, Row) {
    MemBlock mb(100, 5);

    auto row = mb.rows();

    (*row)[4][0] = 4;
    ASSERT_EQ(4, (*row)[4][0].asInt());
}

TEST(MemBlockTest, Mask) {
    MemBlock mb(100, 5);
    auto roww = mb.rows();
    for (int i = 0; i < 100; ++i) {
        (*roww)[i][0] = i;
    }
    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    auto masked = mb.mask(bitmap);

    EXPECT_EQ(3, masked->size());
    auto rows = masked->rows();

    EXPECT_EQ(4, (*rows)[0][0].asInt());
    EXPECT_EQ(20, (*rows)[1][0].asInt());
    EXPECT_EQ(95, (*rows)[2][0].asInt());
}

class ParquetBlockTest : public ::testing::Test {
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

TEST_F(ParquetBlockTest, Column) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);
    auto col = block->col(1);
//    std::vector<int32_t> buffer;
//    for(int i = 0 ;i<100;++i) {
//        buffer.push_back((*col)[i].asInt());
//    }
    EXPECT_EQ(156, (*col)[0].asInt());
    EXPECT_EQ(68, (*col)[1].asInt());
    EXPECT_EQ(64, (*col)[2].asInt());
//    EXPECT_EQ(3, (*col)[3].asInt());
//    EXPECT_EQ(25, (*col)[4].asInt());
//    EXPECT_EQ(16, (*col)[5].asInt());
//    EXPECT_EQ(107, (*col)[6].asInt());
//    EXPECT_EQ(5, (*col)[7].asInt());
//    EXPECT_EQ(20, (*col)[8].asInt());
    EXPECT_EQ(129, (*col)[9].asInt());
    EXPECT_EQ(30, (*col)[10].asInt());
//    EXPECT_EQ(184, (*col)[11].asInt());
    EXPECT_EQ(63, (*col)[12].asInt());
//    EXPECT_EQ(89, (*col)[13].asInt());
//    EXPECT_EQ(109, (*col)[14].asInt());
    EXPECT_EQ(21, (*col)[52].asInt());
    EXPECT_EQ(55, (*col)[53].asInt());
    EXPECT_EQ(95, (*col)[54].asInt());

    auto col2 = block->col(1);
    EXPECT_EQ(156, col2->next().asInt());
    EXPECT_EQ(68, col2->next().asInt());
    EXPECT_EQ(64, col2->next().asInt());
}

class IntDictScanner : public Int32Accessor {
public:
    void processDict(Dictionary<PhysicalType<parquet::Type::INT32> > &) override {
        // Do nothing just maintain a dict
    }

    void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) override {
        // Do nothing just access the page
    }

    Int32Dictionary *accessDict() {
        return this->dict_.get();
    }
};

class StringDictScanner : public ByteArrayAccessor {
public:
    void processDict(Dictionary<ByteArrayType> &) override {
        // Do nothing just maintain a dict
    }

    void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) override {
        // Do nothing just access the page
    }

    ByteArrayDictionary *accessDict() {
        return this->dict_.get();
    }
};


TEST_F(ParquetBlockTest, Raw) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);
    auto scanner = new IntDictScanner();
    block->raw(1, scanner);
    auto dict = scanner->accessDict();
    EXPECT_EQ(200, dict->size());

    EXPECT_EQ(-202, dict->lookup(375));
    EXPECT_EQ(102, dict->lookup(103));
    EXPECT_EQ(-202, dict->lookup(514));
    EXPECT_EQ(18, dict->lookup(19));

    delete scanner;

    auto scanner2 = new StringDictScanner();
    block->raw(10, scanner2);
    auto dict2 = scanner2->accessDict();

    EXPECT_EQ(2266, dict2->size());
    EXPECT_EQ(675, dict2->lookup(ByteArray("1994-02-01")));
    delete scanner2;
}

TEST_F(ParquetBlockTest, Row) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, (1<<14)-1);
    auto rows = block->rows();

    // Test reading and repeatable read
    EXPECT_EQ(1, (*rows)[2][0].asInt());
    EXPECT_EQ(1, (*rows)[2][0].asInt());
    EXPECT_EQ(25, (*rows)[4][1].asInt());
    EXPECT_EQ(25, (*rows)[4][1].asInt());
    EXPECT_EQ(ByteArray("NONE"), *(*rows)[4][13].asByteArray());
    EXPECT_EQ(ByteArray("DELIVER IN PERSON"), *(*rows)[5][13].asByteArray());

    auto rows2 = block->rows();
    DataRow &row = rows2->next();
    EXPECT_EQ(1, row[0].asInt());
    EXPECT_EQ(1, row[0].asInt());
    EXPECT_EQ(156, row[1].asInt());
    EXPECT_EQ(156, row[1].asInt());
    EXPECT_EQ(4, row[2].asInt());
    EXPECT_EQ(4, row[2].asInt());
    row = rows2->next();
    EXPECT_EQ(1, row[0].asInt());
    EXPECT_EQ(68, row[1].asInt());
    EXPECT_EQ(9, row[2].asInt());

//    try {
//        row[3].asInt();
//        FAIL() << "Should not reach here";
//    } catch (const std::invalid_argument &ia) {
//
//    }
}

TEST_F(ParquetBlockTest, Mask) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);

    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    auto masked = block->mask(bitmap);

    EXPECT_EQ(3, masked->size());
    auto rows = masked->rows();
    EXPECT_EQ(1, (*rows)[0][0].asInt());
    EXPECT_EQ(0, rows->pos());
    EXPECT_EQ(3, (*rows)[10][0].asInt());
    EXPECT_EQ(10, rows->pos());
    EXPECT_EQ(32, (*rows)[25][0].asInt());
    EXPECT_EQ(25, rows->pos());

    auto row2 = masked->rows();
    EXPECT_EQ(1, row2->next()[0].asInt());
    EXPECT_EQ(4, row2->pos());
    EXPECT_EQ(7, row2->next()[0].asInt());
    EXPECT_EQ(20, row2->pos());
    EXPECT_EQ(97, row2->next()[0].asInt());
    EXPECT_EQ(95, row2->pos());
}

TEST_F(ParquetBlockTest, MaskOnMask) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);

    shared_ptr<Bitmap> bitmap = make_shared<SimpleBitmap>(100);
    bitmap->put(4);
    bitmap->put(20);
    bitmap->put(95);
    bitmap->put(128);
    bitmap->put(37);

    auto masked = block->mask(bitmap);

    shared_ptr<Bitmap> bitmap2 = make_shared<SimpleBitmap>(100);
    bitmap2->put(20);
    bitmap2->put(95);

    auto masked2 = masked->mask(bitmap2);

    EXPECT_EQ(2, masked->size());
    auto rows = masked->rows();
    EXPECT_EQ(1, (*rows)[0][0].asInt());
    EXPECT_EQ(3, (*rows)[10][0].asInt());
    EXPECT_EQ(32, (*rows)[25][0].asInt());

    auto row2 = masked->rows();
    EXPECT_EQ(7, row2->next()[0].asInt());
    EXPECT_EQ(97, row2->next()[0].asInt());
}

class RawDataAccessorForTest : public Int32Accessor {
public:
    int dict_value_;
    uint64_t pos_ = 0;

    void processDict(Int32Dictionary &dict) override {
        int a = 102;
        dict_value_ = dict_->lookup(a);
    }

    void scanPage(uint64_t numEntry, const uint8_t *data, uint64_t *bitmap, uint64_t bitmap_offset) override {
//        auto dpv1 = static_cast<DataPageV1 *>(page);

        pos_ += numEntry;
    }
};

using namespace parquet;

TEST_F(ParquetBlockTest, RawAndDict) {
    auto block = make_shared<ParquetBlock>(nullptr, rowGroup_, 0, 3);
    shared_ptr<RawDataAccessorForTest> accessor = make_shared<RawDataAccessorForTest>();
    auto bitmap = block->raw(0, accessor.get());

    EXPECT_EQ(0, bitmap->cardinality());
    EXPECT_EQ(29, accessor->dict_value_);
}

TEST(MaskedTableTest, Create) {
    auto ptable = ParquetTable::Open("lineitem", 0x7);

    unordered_map<uint32_t, shared_ptr<Bitmap>> masks;
    masks[0] = make_shared<SimpleBitmap>(1000);
    auto maskTable = make_shared<MaskedTable>(ptable.get(), masks);

    auto round1 = maskTable->blocks()->collect();
    auto round2 = maskTable->blocks()->collect();

    EXPECT_EQ(dynamic_pointer_cast<MaskedBlock>((*round1)[0])->mask()->size(),1000);
    EXPECT_EQ(dynamic_pointer_cast<MaskedBlock>((*round2)[0])->mask()->size(),1000);
}

TEST(MemTableTest, Create) {
    auto mt = MemTable::Make(5);

    mt->allocate(100);
    mt->allocate(200);
    mt->allocate(300);

    auto stream = mt->blocks();

    auto list = *(stream->collect());

    ASSERT_EQ(3, list.size());
    ASSERT_EQ(100, list[0]->size());
    ASSERT_EQ(200, list[1]->size());
    ASSERT_EQ(300, list[2]->size());
}

TEST(TableViewTest, Create) {
    auto parquetTable = ParquetTable::Open("lineitem");
    auto blocks = parquetTable->blocks();
    TableView tableView(parquetTable->numFields(), blocks);

    tableView.blocks()->foreach([](const shared_ptr<Block> &block) { cout << block->size() << endl; });
    tableView.blocks()->foreach([](const shared_ptr<Block> &block) { cout << block->size() << endl; });
}

TEST(DataRowTest, Copy) {

    auto table = MemTable::Make(4);
    auto block = table->allocate(100);
    auto block2 = table->allocate(100);

    vector<MemDataRow> buffer;

    auto rows1 = block->rows();
    auto rows2 = block2->rows();

    srand(time(NULL));

    for (int i = 0; i < 100; ++i) {
        (*rows1)[i][0] = rand();
        buffer.push_back(MemDataRow(4));
        buffer[i] = (*rows1)[i];
        (*rows2)[i] = buffer[i];
        EXPECT_EQ((*rows1)[i][0].asInt(), (*rows2)[i][0].asInt());
    }
}