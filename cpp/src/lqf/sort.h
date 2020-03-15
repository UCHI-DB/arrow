//
// Created by harper on 2/27/20.
//

#ifndef ARROW_SORT_H
#define ARROW_SORT_H

#include "heap.h"
#include "data_model.h"

#define SORTER1(x) [](DataRow *a, DataRow *b) {return (*a)[x].asInt() < (*b)[x].asInt();}
#define SORTER2(x, y) [](DataRow *a, DataRow *b) { auto a0 = (*a)[x].asInt(); auto b0 = (*b)[x].asInt(); return a0 < b0 || (a0 == b0 && (*a)[y].asInt() < (*b)[y].asInt()); }
using namespace std;

namespace lqf {

    /**
     * This sort will copy data twice, once from block to memory buffer, once from memory buffer to sorted buffer.
     * Therefore, it is not supposed to be used on large amount of data.
     */
    class SmallSort {
    private:
        function<bool(DataRow *, DataRow *)> comparator_;
    public:
        SmallSort(function<bool(DataRow *, DataRow *)>);

        shared_ptr<Table> sort(Table &);
    };

    class TopN {
    private:
        uint32_t n_;
        function<bool(DataRow *, DataRow *)> comparator_;
        unique_ptr<Heap<DataRow *>> heap_;
    public:
        TopN(uint32_t, function<bool(DataRow *, DataRow *)>);

        shared_ptr<Table> sort(Table &);

    protected:
        void sortBlock(const shared_ptr<Block> &input);
    };
}
#endif //ARROW_SORT_H
