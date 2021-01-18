//
// Created by harper on 1/17/21.
//
// This benchmark evaluates how much time it costs to read a Parquet column into memory comparing
// to directly read from the Parquet column. In both cases, we read a column and sum up all values.
//
// The purpose of this benchmark is to help making a design decision.
// In current implementation, we load all columns into memory after a HashColumnJoin. This is easier to implement.
// An alternative is to postpone the loading until the data is actually needed. This requires an
// abstraction layer of Column, with two subclasses: MemColumn and ParquetColumn.
// If loading data into memory does not take much time, we can stick to current implementation.
//
// Result shows below:
// Selection Rate     Direct    MemCache
// 0.01               923818      941010
// 0.1               3120949     3500252
// 0.5              13276720    15437441
// Even with selection rate of 0.5, reading a column within one block into memory only takes 2ms, which is acceptable
//
// Conclusion:

#include <vector>
#include <benchmark/benchmark.h>
#include "tpch/tpchquery.h"
#include "data_model.h"

using namespace std;
using namespace lqf;

double mask_ratio = 0.5;

static shared_ptr<Bitmap> mask_;

static double sum_;

static function<shared_ptr<Block>(const shared_ptr<Block> &)> mapper_ = [](const shared_ptr<Block> &block) {
    auto size = block->size();
    auto mask = make_shared<SimpleBitmap>(size);

    for (uint32_t i = 0; i < size; ++i) {
        auto sample = (double) rand() / RAND_MAX;
        if (sample < mask_ratio) {
            mask->put(i);
        }
    }
    auto masked = block->mask(mask);
    return masked;
};

void init() {
    auto lineitem = ParquetTable::Open(tpch::LineItem::path, {});
    srand(time(NULL));
    auto blocks = lineitem->blocks()->collect();
    auto first_block = (*blocks)[0];
    auto size = first_block->size();
    mask_ = make_shared<SimpleBitmap>(size);

    for (uint32_t i = 0; i < size; ++i) {
        auto sample = (double) rand() / RAND_MAX;
        if (sample < mask_ratio) {
            mask_->put(i);
        }
    }
}


void MemTableBenchmark_Direct(benchmark::State &state) {
    init();
    for (auto _:state) {
        auto lineitem = ParquetTable::Open(tpch::LineItem::path, {tpch::LineItem::DISCOUNT});
        auto blocks = lineitem->blocks()->collect();
        auto first_block = (*blocks)[0];
        auto masked = first_block->mask(mask_);
        auto block_size = masked->size();
        auto col = masked->col(tpch::LineItem::DISCOUNT);
        for (uint32_t i = 0; i < block_size; ++i) {
            sum_ += (*col)[i].asDouble();
        }
    }
}

void MemTableBenchmark_MemCached(benchmark::State &state) {
    init();
    for (auto _:state) {
        auto lineitem = ParquetTable::Open(tpch::LineItem::path, {tpch::LineItem::DISCOUNT});
        auto blocks = lineitem->blocks()->collect();
        auto first_block = (*blocks)[0];
        auto masked = first_block->mask(mask_);
        auto block_size = masked->size();
        auto col = masked->col(tpch::LineItem::DISCOUNT);
        std::vector<double> buffer(block_size);
        for (uint32_t i = 0; i < block_size; ++i) {
            buffer[i] = (*col)[i].asDouble();
        }
        for (uint32_t i = 0; i < block_size; ++i) {
            sum_ += buffer[i];
        }
    }
}

BENCHMARK(MemTableBenchmark_Direct);
BENCHMARK(MemTableBenchmark_MemCached);

BENCHMARK_MAIN();