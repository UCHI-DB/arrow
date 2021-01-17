//
// Created by harper on 4/4/20.
//

// This benchmark tests if aggregation on MemvTable is faster or slower than MemTable
#include <benchmark/benchmark.h>
#include "join.h"

using namespace lqf;

static int table_size = 10000000;
static int key_size =   1000;
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

    auto mod = (int) (right_size / mod_ratio);
    for (int i = 0; i < left_size; ++i) {
        auto val = abs(rand()) % key_size;
        (*rowWriter)[i][0] = val;
        (*rowWriter)[i][1] = rand();
        (*rowWriter)[i][2] = rand();
        (*rowWriter)[i][3] = rand();
        (*rowWriter)[i][4] = rand();
        (*colWriter)[i][0] = val;
        (*colWriter)[i][1] = rand();
        (*colWriter)[i][2] = rand();
        (*colWriter)[i][3] = rand();
        (*colWriter)[i][4] = rand();
    }

}

static void AggBenchmark_Row(benchmark::State &state) {
    init();
    function<vector<AggField *>()> aggFields = []() {
        return vector<AggField *>{new DoubleSum(1)};
    };
    for (auto _:state) {
        HashLargeAgg agg(hasher, RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(), aggFields);
        auto agged = agg.agg(*withPart);
        size_ = agged->size();
    }
}

//READ HASHMAP
//QUERY BITMAP

static void AggBenchmark_Column(benchmark::State &state) {
    init();
    for (auto _ : state) {
        HashColumnJoin join(0, 0, new ColumnBuilder({JL(1), JL(2), JL(3), JL(4), JR(1)}), true);
        size_ = join.join(*leftColTable_, *rightTable_)->size();
    }
}

BENCHMARK(AggBenchmark_Row);
// Note
BENCHMARK(AggBenchmark_Column);

BENCHMARK_MAIN();