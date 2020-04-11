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
        auto resultTable = MemTable::Make(table.colSize());

        vector<DataRow *> collector;

        function<void(const shared_ptr<Block> &)> proc = bind(&TopN::sortBlock, this, &collector, resultTable.get(),
                                                              _1);
        table.blocks()->foreach(proc);

        /// Make a final sort and fetch the top n
        if (collector.size() > n_) {
            std::sort(collector.begin(), collector.end(), comparator_);
        }

        auto resultBlock = resultTable->allocate(n_);
        auto resultRows = resultBlock->rows();
        for (uint32_t i = 0; i < n_; ++i) {
            (*resultRows)[i] = *(collector[i]);
            delete collector[i];
        }

        return resultTable;
    }

    void TopN::sortBlock(vector<DataRow *> *collector, MemTable *dest, const shared_ptr<Block> &input) {
        Heap<DataRow *> heap(n_, [=]() { return new MemDataRow(dest->colOffset()); }, comparator_);
        auto rows = input->rows();
        auto block_size = input->size();
        for (uint32_t i = 0; i < block_size; ++i) {
            DataRow &row = rows->next();
            heap.add(&row);
        }
        heap.done();

        /// Collect the result
        collector_lock_.lock();
        auto content = heap.content();
        collector->insert(collector->end(), content.begin(), content.end());
        collector_lock_.unlock();
    }

    SmallSort::SmallSort(function<bool(DataRow *, DataRow *)> comp) : comparator_(comp) {}

    shared_ptr<Table> SmallSort::sort(Table &table) {
        const vector<uint32_t> &col_size = table.colSize();
        vector<uint32_t> col_offset;
        col_offset.push_back(0);
        for (auto &i:col_size) {
            col_offset.push_back(i + col_offset.back());
        }

        vector<DataRow *> sortingRows;

        table.blocks()->foreach([&sortingRows](const shared_ptr<Block> &block) {
            auto inputRows = block->rows();
            uint32_t block_size = block->size();
            for (uint32_t i = 0; i < block_size; ++i) {
                sortingRows.push_back(inputRows->next().snapshot().release());
            }
        });

        std::sort(sortingRows.begin(), sortingRows.end(), comparator_);

        auto resultTable = MemTable::Make(table.colSize());
        auto resultBlock = resultTable->allocate(sortingRows.size());
        auto resultRows = resultBlock->rows();

        auto num_rows = sortingRows.size();
        for (uint32_t i = 0; i < num_rows; ++i) {
            (*resultRows)[i] = *sortingRows[i];
            delete sortingRows[i];
        }
        return resultTable;
    }
}
