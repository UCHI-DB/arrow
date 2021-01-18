//
// Created by harper on 4/4/20.
//

// This benchmark tests if aggregation on MemvTable is faster or slower than MemTable
#include <benchmark/benchmark.h>
#include "join.h"
#include "agg.h"

using namespace lqf;
using namespace lqf::agg;
static int table_size = 10000000;
static int key_size = 1000;
static shared_ptr<MemTable> rowTable_;
static shared_ptr<MemTable> colTable_;
static uint64_t size_;

void init() {
    rowTable_ = MemTable::Make(5, false);
    colTable_ = MemTable::Make(5, true);

    auto rowBlock = rowTable_->allocate(table_size);
    auto colBlock = colTable_->allocate(table_size);
    srand(time(NULL));

    auto rowWriter = rowBlock->rows();
    auto colWriter = colBlock->rows();

    for (int i = 0; i < table_size; ++i) {
        auto val = abs(rand()) % key_size;
        (*rowWriter)[i][0] = val;
        (*rowWriter)[i][1] = val;
        (*rowWriter)[i][2] = rand();
        (*rowWriter)[i][3] = rand();
        (*rowWriter)[i][4] = rand();
        (*colWriter)[i][0] = val;
        (*colWriter)[i][1] = val;
        (*colWriter)[i][2] = rand();
        (*colWriter)[i][3] = rand();
        (*colWriter)[i][4] = rand();
    }

}

static void AggBenchmark_Row(benchmark::State &state) {
    init();
    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>{new DoubleSum(2)};
    };
    for (auto _:state) {
        HashLargeAgg agg(COL_HASHER2(0, 1),
                         RowCopyFactory().field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)->buildSnapshot(), aggFields);
        auto agged = agg.agg(*rowTable_);
        size_ = agged->size();
    }
}

//READ HASHMAP
//QUERY BITMAP

static void AggBenchmark_Column(benchmark::State &state) {
    init();
    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>{new DoubleSum(2)};
    };
    for (auto _:state) {
        HashLargeAgg agg(COL_HASHER2(0, 1),
                         RowCopyFactory().field(F_REGULAR, 0, 0)->field(F_REGULAR, 1, 1)->buildSnapshot(), aggFields);
        auto agged = agg.agg(*colTable_);
        size_ = agged->size();
    }
}

BENCHMARK(AggBenchmark_Row)->MinTime(5);
// Note
BENCHMARK(AggBenchmark_Column)->MinTime(5);

BENCHMARK_MAIN();