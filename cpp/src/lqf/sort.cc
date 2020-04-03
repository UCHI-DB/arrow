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
        auto resultTable = MemTable::Make(table.numFields(), table.numStringFields());
        auto tablePointer = resultTable.get();


        rowMaker_ = [tablePointer]() { return new MemDataRow(tablePointer->colOffset()); };

        vector<DataRow *> collector;

        function<void(const shared_ptr<Block> &)> proc = bind(&TopN::sortBlock, this, &collector, _1);
        table.blocks()->foreach(proc);

        /// Make a final sort and fetch the top n
        if (collector.size() > n_) {
            std::sort(collector.begin(), collector.end(), comparator_);
        }

        auto resultBlock = resultTable->allocate(n_);
        auto resultRows = resultBlock->rows();
        for (uint32_t i = 0; i < n_; ++i) {
            (*resultRows)[i] = *(collector[i]);
        }
        // Clean up
        for (auto &row:collector) {
            delete row;
        }

        return resultTable;
    }

    void TopN::sortBlock(vector<DataRow *> *collector, const shared_ptr<Block> &input) {
        Heap<DataRow *> heap(n_, rowMaker_, comparator_);
        auto rows = input->rows();
        for (uint32_t i = 0; i < input->size(); ++i) {
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
