//
// Created by harper on 2/27/20.
//

#include "sort.h"

namespace lqf {

    using namespace std;
    using namespace std::placeholders;

    TopN::TopN(uint32_t n, uint32_t numFields, function<int32_t(DataRow * , DataRow * )> comp)
            : n_(n), numFields_(numFields), comparator_(comp),
              heap_(n_, [=]() { return new MemDataRow(numFields_); },
                    comparator_) {
    }

    shared_ptr <Table> TopN::sort(Table &table) {
        auto resultTable = MemTable::Make(numFields_);
        auto resultBlock = resultTable->allocate(n_);

        vector < DataRow * > &heapContainer = heap_.content();

        function<void(shared_ptr < Block > &)> proc = bind(&TopN::sortBlock, this, _1);
        table.blocks()->foreach(proc);

        heap_.done();

        auto resultRows = resultBlock->rows();
        for (uint32_t i = 0; i < n_; ++i) {
            (*resultRows)[i] = *(heapContainer[i]);
            delete heapContainer[i];
        }

        return resultTable;
    }

    void TopN::sortBlock(shared_ptr <Block> &input) {
        auto rows = input->rows();
        for (uint32_t i = 0; i < input->size(); ++i) {
            DataRow &row = rows->next();
            heap_.add(&row);
        }
    }
}
