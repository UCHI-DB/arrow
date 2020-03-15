//
// Created by harper on 2/27/20.
//

#include <memory>
#include "sort.h"

using namespace std;
using namespace std::placeholders;

namespace lqf {

    TopN::TopN(uint32_t n, function<bool(DataRow *, DataRow *)> comp)
            : n_(n), comparator_(comp) {}

    shared_ptr<Table> TopN::sort(Table &table) {
        uint32_t num_fields = table.numFields();
        auto resultTable = MemTable::Make(num_fields);
        auto resultBlock = resultTable->allocate(n_);

        heap_ = unique_ptr<Heap<DataRow *>>(
                new Heap<DataRow *>(n_, [=]() { return new MemDataRow(num_fields); }, comparator_));

        vector<DataRow *> &heapContainer = heap_->content();

        function<void(const shared_ptr<Block> &)> proc = bind(&TopN::sortBlock, this, _1);
        table.blocks()->foreach(proc);

        heap_->done();

        auto resultRows = resultBlock->rows();
        for (uint32_t i = 0; i < n_; ++i) {
            (*resultRows)[i] = *(heapContainer[i]);
            delete heapContainer[i];
        }

        return resultTable;
    }

    void TopN::sortBlock(const shared_ptr<Block> &input) {
        auto rows = input->rows();
        for (uint32_t i = 0; i < input->size(); ++i) {
            DataRow &row = rows->next();
            heap_->add(&row);
        }
    }

    SmallSort::SmallSort(function<bool(DataRow *, DataRow *)> comp) : comparator_(comp) {}

    shared_ptr<Table> SmallSort::sort(Table &table) {
        uint32_t num_fields = table.numFields();
        vector<MemDataRow *> sortingRows;
        table.blocks()->foreach([&sortingRows, num_fields](const shared_ptr<Block> &block) {
            auto inputRows = block->rows();
            for (uint32_t i = 0; i < block->size(); ++i) {
                MemDataRow *copy = new MemDataRow(num_fields);
                *copy = inputRows->next();
                sortingRows.push_back(copy);
            }
        });

        std::sort(sortingRows.begin(), sortingRows.end(), comparator_);

        auto resultTable = MemTable::Make(table.numFields());
        auto resultBlock = resultTable->allocate(sortingRows.size());
        auto resultRows = resultBlock->rows();

        for (uint32_t i = 0; i < sortingRows.size(); ++i) {
            (*resultRows)[i] = *sortingRows[i];
        }
        return resultTable;
    }
}
