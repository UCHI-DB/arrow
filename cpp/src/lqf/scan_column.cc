//
// Created by harper on 2/9/20.
//
#include <array>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <fstream>
#include <parquet/api/reader.h>
#include <parquet/file_reader.h>
#include "bitmap.h"
#include "data_model.h"
#include "tpch/tpch_query.h"
#include "filter.h"

bool predicate(int32_t value) {
    return value % 20 == 0;
}

void scanTable2() {

    std::ofstream output;
    output.open("/home/harper/cpp_res");
    const char *fileName = "/local/hajiang/tpch/5/lineitem/lineitem.parquet";

    using namespace parquet;
    using namespace lqf;

    auto dateFrom = ByteArray("1998-09-01");
    auto dictless = new lqf::sboost::ByteArrayDictLess(lqf::tpch::LineItem::SHIPDATE, dateFrom);

    auto table = ParquetTable::Open(fileName, {tpch::LineItem::SHIPDATE});
    table->blocks()->foreach([=](const shared_ptr<Block> &block) {
//        auto col = block->col(lqf::tpch::LineItem::SHIPDATE);
//        for(uint i = 0 ; i < block->size();++i) {
//            std::cout << (*col)(i).asInt() << std::endl;
//        }

        auto pblock = static_pointer_cast<ParquetBlock>(block);
        pblock->raw(lqf::tpch::LineItem::SHIPDATE, dictless);
    });

    delete dictless;
}

void scanTable3() {
    std::ofstream output;
    output.open("/home/harper/cpp_res");
    const char *fileName = "/local/hajiang/tpch/5/lineitem/lineitem.parquet";

    using namespace parquet;

    auto fileReader = ParquetFileReader::OpenFile(fileName);
    auto rowGroup = fileReader->RowGroup(0);
    auto reader1 = rowGroup->Column(0);
    int32_t size = sizeof(uint32_t) * rowGroup->metadata()->num_rows();
    void *buffer1 = malloc(sizeof(uint32_t) * rowGroup->metadata()->num_rows());
    int64_t value_read;
    reader1->ReadBatch(rowGroup->metadata()->num_rows(), 0, 0, buffer1, &value_read);
    auto reader2 = rowGroup->Column(0);
    void *buffer2 = malloc(sizeof(uint32_t) * rowGroup->metadata()->num_rows());
    reader2->ReadBatch(rowGroup->metadata()->num_rows(), 0, 0, buffer2, &value_read);
    for(int i = 0 ; i < size;i++) {
        assert(((char*)buffer1)[i] == ((char*)buffer2)[i]);
    }
    return;
}

void scanTable() {
    auto start = std::chrono::high_resolution_clock::now();

    std::ofstream output;
    output.open("/home/harper/cpp_res");
    const char *fileName = "/home/harper/tpch/lqf/5/lineitem/lineitem.parquet";
    int filterIndex = 2;
    int displayIndex = 1;

    using namespace parquet;
    using namespace lqf;
    auto fileReader = ParquetFileReader::OpenFile(std::string(fileName));
    auto metadata = fileReader->metadata();
    int numRowGroup = metadata->num_row_groups();

    const uint32_t batchSize = 1000;
    std::vector<int32_t> buffer(batchSize);
    int result[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    for (int rg_idx = 0; rg_idx < numRowGroup; rg_idx++) {

        auto rowGroup = fileReader->RowGroup(rg_idx);
        auto rowGroupSize = rowGroup->metadata()->num_rows();
        Bitmap *bitmap = new SimpleBitmap(rowGroupSize);
        auto columnReader = std::static_pointer_cast<Int32Reader>(rowGroup->Column(filterIndex));
        int64_t values_read = 0;

        // Read data from column A and generate bitmap
        int64_t offset = 0;
        while (offset < rowGroupSize) {
            uint32_t num_read_values = columnReader->ReadBatch(batchSize, nullptr, nullptr, buffer.data(),
                                                               &values_read);
            for (uint32_t idx = 0; idx < num_read_values; idx++) {
                if (predicate(buffer[idx])) {
                    bitmap->put(offset + idx);
                }
            }
            offset += num_read_values;
        }

        // Use Bitmap to fetch data from column B


        auto itr = bitmap->iterator();

        auto secondColumnReader = std::static_pointer_cast<Int32Reader>(rowGroup->Column(displayIndex));
        int32_t value = 0;
        while (itr->hasNext()) {
            uint64_t pos = itr->next();
            secondColumnReader->MoveTo(pos);
//            printf("%d,%lu\n", rg_idx, pos);
            secondColumnReader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
            // Print out value
            output << pos << ":" << value << std::endl;
            result[value % 10] += 1;
        }
    }
    for (int i = 0; i < 10; ++i) {
        printf("%d\n", result[i]);
    }
//    auto rowGroup = fileReader->RowGroup(0);
//    auto secondColumnReader = std::static_pointer_cast<Int32Reader>(rowGroup->Column(displayIndex));
//    int32_t value = 0;
//    int64_t values_read = 0;
////    for (int i = 0; i < 100; i++) {
////        secondColumnReader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//////            // Print out value
////        printf("%d:%d\n", i, value);
////    }
////    secondColumnReader->MoveTo(2);
////    secondColumnReader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
////    printf("%d\n", value);
//    secondColumnReader->MoveTo(6145);
//    secondColumnReader->ReadBatch(1, nullptr, nullptr, &value, &values_read);
//    printf("%d\n", value);

    output.close();

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

// To get the value of duration use the count()
// member function on the duration object
    std::cout << duration.count() << std::endl;
}

int main(int argc, char **argv) {
    scanTable3();
}