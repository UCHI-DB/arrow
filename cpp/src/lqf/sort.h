//
// Created by harper on 2/27/20.
//

#ifndef ARROW_SORT_H
#define ARROW_SORT_H

#include "heap.h"
#include "data_model.h"

#define SORTER1(x) [](DataRow *a, DataRow *b) {return (*a)[x].asInt() < (*b)[x].asInt();}
#define SORTER2(x, y) [](DataRow *a, DataRow *b) { auto a0 = (*a)[x].asInt(); auto b0 = (*b)[x].asInt(); return a0 < b0 || (a0 == b0 && (*a)[y].asInt() < (*b)[y].asInt()); }
#define SDGE(x) (*a)[x].asDouble() > (*b)[x].asDouble()
#define SDLE(x) (*a)[x].asDouble() < (*b)[x].asDouble()
#define SDE(x) (*a)[x].asDouble() == (*b)[x].asDouble()
#define SBLE(x) (*a)[x].asByteArray() < (*b)[x].asByteArray()
#define SBE(x) (*a)[x].asByteArray() == (*b)[x].asByteArray()
#define SILE(x) (*a)[x].asInt() < (*b)[x].asInt()
#define SIGE(x) (*a)[x].asInt() > (*b)[x].asInt()
#define SIE(x) (*a)[x].asInt() == (*b)[x].asInt()
using namespace std;

namespace lqf {

    /**
     * This sort will copy data twice, once from block to memory buffer, once from memory buffer to sorted buffer.
     * Therefore, it is not supposed to be used on large amount of data.
     */
    class SmallSort {
    private:
        function<bool(DataRow *, DataRow *)> comparator_;

        function<unique_ptr<MemDataRow>(DataRow &)> snapshoter_;
    public:
        SmallSort(function<bool(DataRow *, DataRow *)>);

        SmallSort(function<bool(DataRow *, DataRow *)>, function<unique_ptr<MemDataRow>(DataRow &)>);

        shared_ptr<Table> sort(Table &);
    };

    class TopN {
    private:
        uint32_t n_;
        mutex collector_lock_;
        function<bool(DataRow *, DataRow *)> comparator_;
        function<DataRow *()> rowMaker_;
    public:
        TopN(uint32_t, function<bool(DataRow *, DataRow *)>);

        shared_ptr<Table> sort(Table &);

    protected:
        void sortBlock(vector<DataRow *> *, const shared_ptr<Block> &input);
    };
}
#endif //ARROW_SORT_H
