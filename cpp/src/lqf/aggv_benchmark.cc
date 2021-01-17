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
        auto val = abs(rand()) % mod;
        (*leftRowWriter)[i][0] = val;
        (*leftRowWriter)[i][1] = 0;
        (*leftRowWriter)[i][2] = 0;
        (*leftRowWriter)[i][3] = 0;
        (*leftRowWriter)[i][4] = 0;
        (*leftColWriter)[i][0] = val;
        (*leftColWriter)[i][1] = 0;
        (*leftColWriter)[i][2] = 0;
        (*leftColWriter)[i][3] = 0;
        (*leftColWriter)[i][4] = 0;
    }

    auto rightBlock = rightTable_->allocate(right_size);
    auto rightWriter = rightBlock->rows();
    for (int i = 0; i < right_size; ++i) {
        (*rightWriter)[i][0] = i;
        (*rightWriter)[i][1] = 0;
    }
}

static void HashJoinBenchmark_Row(benchmark::State &state) {
    init();
    for (auto _:state) {
        HashJoin join(0, 0, new RowBuilder({JL(1), JL(2), JL(3), JL(4), JR(1)}));
        size_ = join.join(*leftRowTable_, *rightTable_)->size();
    }
}

//READ HASHMAP
//QUERY BITMAP

static void HashJoinBenchmark_Column(benchmark::State &state) {
    init();
    for (auto _ : state) {
        HashColumnJoin join(0, 0, new ColumnBuilder({JL(1), JL(2), JL(3), JL(4), JR(1)}), true);
        size_ = join.join(*leftColTable_, *rightTable_)->size();
    }
}

BENCHMARK(HashJoinBenchmark_Row);
// Note
BENCHMARK(HashJoinBenchmark_Column);

BENCHMARK_MAIN();