//
// Created by harper on 4/4/20.
//

// This benchmark tests if aggregation on MemvTable is faster or slower than MemTable
#include <benchmark/benchmark.h>
#include "join.h"

using namespace lqf;

static int table_size = 10000000;
static int key_size =   1000;
static shared_ptr<MemTable> leftRowTable_;
static shared_ptr<MemTable> leftColTable_;
static shared_ptr<MemTable> rightTable_;
static uint64_t size_;

void init() {
    leftRowTable_ = MemTable::Make(5, false);
    leftColTable_ = MemTable::Make(5, true);
    rightTable_ = MemTable::Make(2, false);

    auto leftRowBlock = leftRowTable_->allocate(left_size);
    auto leftColBlock = leftColTable_->allocate(left_size);
    srand(time(NULL));

    auto leftRowWriter = leftRowBlock->rows();
    auto leftColWriter = leftColBlock->rows();

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