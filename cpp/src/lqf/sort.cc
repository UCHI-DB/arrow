//
// Created by harper on 2/27/20.
//

#include <memory>
#include "sort.h"

using namespace std;
using namespace std::placeholders;

namespace lqf {

    SmallSort::SmallSort(function<bool(DataRow *, DataRow *)> comp, bool vertical)
            : Node(1), comparator_(comp), vertical_(vertical) {}

    unique_ptr<NodeOutput> SmallSort::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = sort(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> SmallSort::sort(Table &table) {
        auto output = MemTable::Make(table.colSize(), false);

        auto block = make_shared<MemListBlock>();
        auto container = &block->content();
        table.blocks()->foreach([container](const shared_ptr<Block> &block) {
            auto rows = block->rows();
            auto block_size = block->size();
            for (uint32_t i = 0; i < block_size; ++i) {
                container->push_back(rows->next().snapshot().release());
            }
        });
        std::sort(container->begin(), container->end(), comparator_);
        output->append(block);
        return output;
    }

    SnapshotSort::SnapshotSort(const vector<uint32_t> col_size,
                               function<bool(DataRow *, DataRow *)> comp,
                               function<unique_ptr<MemDataRow>(DataRow &)> snapshoter, bool vertical)
            : Node(1), col_size_(col_size), comparator_(comp), snapshoter_(snapshoter), vertical_(vertical) {}

    unique_ptr<NodeOutput> SnapshotSort::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = sort(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> SnapshotSort::sort(Table &input) {
        auto memblock = make_shared<MemListBlock>();
        auto row_cache = &(memblock->content());
        input.blocks()->sequential()->foreach([this, row_cache](const shared_ptr<Block> &block) {
            auto block_size = block->size();
            auto rows = block->rows();
            for (uint32_t i = 0; i < block_size; ++i) {
                row_cache->push_back(snapshoter_(rows->next()).release());
            }
        });

        std::sort(row_cache->begin(), row_cache->end(), comparator_);
        auto resultTable = MemTable::Make(col_size_, vertical_);
        resultTable->append(memblock);
        return resultTable;
    }

    TopN::TopN(uint32_t n, function<bool(DataRow *, DataRow *)> comp, bool vertical)
            : Node(1), n_(n), comparator_(comp), vertical_(vertical) {}

    unique_ptr<NodeOutput> TopN::execute(const vector<NodeOutput *> &inputs) {
        auto input0 = static_cast<TableOutput *>(inputs[0]);
        auto table = sort(*(input0->get()));
        return unique_ptr<TableOutput>(new TableOutput(table));
    }

    shared_ptr<Table> TopN::sort(Table &table) {
        auto resultTable = MemTable::Make(table.colSize(), vertical_);

        vector<DataRow *> collector;

        function<void(const shared_ptr<Block> &)> proc =
                bind(&TopN::sortBlock, this, &collector, resultTable.get(), _1);
        table.blocks()->foreach(proc);

        /// Make a final sort and fetch the top n
        if (collector.size() > n_) {
            std::sort(collector.begin(), collector.end(), comparator_);
        }

        auto result_size = std::min(n_, static_cast<uint32_t>(collector.size()));
        auto resultBlock = resultTable->allocate(result_size);
        auto resultRows = resultBlock->rows();

        for (uint32_t i = 0; i < result_size; ++i) {
            (*resultRows)[i] = *(collector[i]);
        }
        for (auto &item:collector) {
            delete item;
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
        // So heap will not delete these pointers as they have been moved
        heap.content().clear();
        collector_lock_.unlock();
    }
}
