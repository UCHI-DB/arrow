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

    SmallSort::SmallSort(function<bool(DataRow *, DataRow *)> comp) : comparator_(comp), snapshoter_(nullptr) {}

    SmallSort::SmallSort(function<bool(DataRow *, DataRow *)> comp,
                         function<unique_ptr<MemDataRow>(DataRow &)> snapshot)
            : comparator_(comp), snapshoter_(snapshot) {}

    shared_ptr<Table> SmallSort::sort(Table &table) {
        const vector<uint32_t> &col_size = table.colSize();
        vector<uint32_t> col_offset;
        col_offset.push_back(0);
        for (auto &i:col_size) {
            col_offset.push_back(i + col_offset.back());
        }

        vector<MemDataRow*> sortingRows;
        if (snapshoter_) {
            table.blocks()->foreach([&sortingRows, this](const shared_ptr<Block> &block) {
                auto inputRows = block->rows();
                for (uint32_t i = 0; i < block->size(); ++i) {
                    unique_ptr<MemDataRow> copy = snapshoter_(inputRows->next());
                    sortingRows.push_back(copy.release());
                }
            });
        } else {
            table.blocks()->foreach([&sortingRows, &col_offset](const shared_ptr<Block> &block) {
                auto inputRows = block->rows();
                for (uint32_t i = 0; i < block->size(); ++i) {
                    MemDataRow *copy = new MemDataRow(col_offset);
                    *copy = inputRows->next();
                    sortingRows.push_back(copy);
                }
            });
        }

        std::sort(sortingRows.begin(), sortingRows.end(), comparator_);

        auto resultTable = MemTable::Make(table.colSize());
        auto resultBlock = resultTable->allocate(sortingRows.size());
        auto resultRows = resultBlock->rows();

        for (uint32_t i = 0; i < sortingRows.size(); ++i) {
            (*resultRows)[i] = *sortingRows[i];
            delete sortingRows[i];
        }
        return resultTable;
    }
}
