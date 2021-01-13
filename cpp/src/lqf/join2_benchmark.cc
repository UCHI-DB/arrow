//
// This benchmark compares HashColumnJoin vs HashJoin
//
// We perform a join on a large table against a medium size table.
// The left table get 5 columns. The right table get two columns.
//
// One item uses vertical table for the left. One item uses horizontal.
//
// Created by Harper on 1/12/21.
//

#include <benchmark/benchmark.h>
#include "join.h"

using namespace lqf;

int left_size = 10000000;
int right_size = 100000;
double mod_ratio = 0.7;

class HashJoinBenchmark : public benchmark::Fixture {
protected:
    shared_ptr<MemTable> leftRowTable_;
    shared_ptr<MemTable> leftColTable_;
    shared_ptr<MemTable> rightTable_;
    uint64_t size_;
public:
    HashJoinBenchmark() {
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
};

BENCHMARK_F(HashJoinBenchmark, Row)(benchmark::State &state) {
    for (auto _:state) {
        HashJoin join(0, 0, new RowBuilder({JL(1), JL(2), JL(3), JL(4), JR(1)}));
        size_ = join.join(*leftRowTable_, *rightTable_)->size();
    }
}

//READ HASHMAP
//QUERY BITMAP

BENCHMARK_F(HashJoinBenchmark, Column)(benchmark::State &state) {
    for (auto _ : state) {
        HashColumnJoin join(0, 0, new ColumnBuilder({JL(1), JL(2), JL(3), JL(4), JR(1)}), true);
        size_ = join.join(*leftColTable_, *rightTable_)->size();
    }
}