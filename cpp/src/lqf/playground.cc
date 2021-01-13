//
// Created by Harper on 12/28/20.
//

#include <iostream>
#include <cuckoohash_map.hh>
#include <sparsehash/dense_hash_map>
#include "data_model.h"
#include "join.h"

using namespace std;
using namespace libcuckoo;
using namespace google;
using namespace lqf;

int main() {

    int left_size = 10000000;
    int right_size = 100000;
    double mod_ratio = 0.7;


    shared_ptr<MemTable> leftRowTable_;
    shared_ptr<MemTable> leftColTable_;
    shared_ptr<MemTable> rightTable_;
    uint64_t size_;

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

    for (auto i = 0; i < 10; ++i) {
        HashJoin join(0, 0, new RowBuilder({JL(1), JL(2), JL(3), JL(4), JR(1)}));
        size_ = join.join(*leftRowTable_, *rightTable_)->size();

        cout << size_ << endl;
    }
}