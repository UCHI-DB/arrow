//
// Created by harper on 4/6/20.
//

#include <parquet/types.h>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/agg.h>
#include <lqf/sort.h>
#include <lqf/print.h>
#include <lqf/join.h>
#include <lqf/mat.h>
#include "tpchquery.h"


namespace lqf {
    namespace tpch {
        using namespace agg;
        using namespace sboost;
        namespace q21 {
            class ItemJoin : public Join {
            protected:
                unordered_set<int32_t> denied_;
                unordered_map<int32_t, pair<int32_t, int32_t>> container_;
                mutex map_lock_;

            public:
                shared_ptr<Table> join(Table &item2, Table &item1) {

                    shared_ptr<MemTable> output = MemTable::Make(vector<uint32_t>{1, 1});
                    uint32_t max_size = 0;
                    item1.blocks()->sequential()->foreach([this](const shared_ptr<Block> &block) {
                        auto orderkeys = block->col(LineItem::ORDERKEY);
                        auto suppkeys = block->col(LineItem::SUPPKEY);
                        for (uint32_t i = 0; i < block->size(); ++i) {
                            int orderkey = orderkeys->next().asInt();
                            int suppkey = suppkeys->next().asInt();
                            if (denied_.find(orderkey) == denied_.end()) {
                                auto exist = container_.find(orderkey);
                                if (exist != container_.end()) {
                                    if ((*exist).second.first == suppkey) {
                                        (*exist).second.second++;
                                    } else {
                                        denied_.insert(orderkey);
                                        container_.erase(exist);
                                    }
                                } else {
                                    container_[orderkey] = pair<int32_t, int32_t>(suppkey, 1);
                                }
                            }
                        }
                    });
                    for (auto &item:container_) {
                        max_size += item.second.second;
                    }

                    item2.blocks()->foreach([this, &output, &max_size](const shared_ptr<Block> &block) {
                        auto output_block = output->allocate(max_size);
                        auto write_rows = output_block->rows();
                        uint32_t counter = 0;

                        auto orderkeys = block->col(LineItem::ORDERKEY);
                        auto suppkeys = block->col(LineItem::SUPPKEY);
                        for (uint32_t i = 0; i < block->size(); ++i) {
                            int orderkey = orderkeys->next().asInt();
                            int suppkey = suppkeys->next().asInt();
                            auto ite = container_.find(orderkey);
                            if (ite != container_.end() && ite->second.first != suppkey) {
                                int found = ite->second.first;
                                int count = ite->second.second;
                                map_lock_.lock();
                                container_.erase(ite);
                                map_lock_.unlock();
                                for (int j = 0; j < count; ++j) {
                                    DataRow &row = (*write_rows)[counter++];
                                    row[0] = orderkey;
                                    row[1] = found;
                                }
                            }
                        }
                        output_block->resize(counter);
                    });
                    return output;
                }
            };

            ByteArray status("F");

        }
        using namespace q21;

        void executeQ21() {
            auto order = ParquetTable::Open(Orders::path, {Orders::ORDERKEY, Orders::ORDERSTATUS});
            auto lineitem = ParquetTable::Open(LineItem::path,
                                               {LineItem::SUPPKEY, LineItem::ORDERKEY, LineItem::RECEIPTDATE,
                                                LineItem::COMMITDATE});
            auto supplier = ParquetTable::Open(Supplier::path, {Supplier::SUPPKEY, Supplier::NATIONKEY});


            ColFilter orderFilter(new SboostPredicate<ByteArrayType>(Orders::ORDERSTATUS,
                                                                     bind(&ByteArrayDictEq::build, status)));
            auto validorder = orderFilter.filter(*order);

            HashFilterJoin lineWithOrderJoin(LineItem::ORDERKEY, Orders::ORDERKEY);
            auto l1withorder = lineWithOrderJoin.join(*lineitem, *validorder);

            RowFilter linedateFilter([](DataRow &row) {
                return row[LineItem::RECEIPTDATE].asByteArray() > row[LineItem::COMMITDATE].asByteArray();
            });
            auto l1 = linedateFilter.filter(*l1withorder);

            ItemJoin itemExistJoin;
            // ORDERKEY, SUPPKEY
            auto ordersuppkey = itemExistJoin.join(*lineitem, *l1);

            HashAgg countAgg(vector<uint32_t>{1, 1}, {AGI(1)}, []() { return vector<AggField *>{new Count()}; },
                             COL_HASHER(1));
            countAgg.useVertical();
            // SUPPKEY, COUNT
            auto agged = countAgg.agg(*ordersuppkey);

            ColFilter supplierNationFilter(new SboostPredicate<Int32Type>(Supplier::NATIONKEY,
                                                                          bind(&Int32DictEq::build, 3)));
            auto validSupplier = supplierNationFilter.filter(*supplier);

            HashJoin lineitemSupplierFilter(0, Supplier::SUPPKEY, new RowBuilder({JL(1), JRS(Supplier::NAME)}));
            // COUNT SUPPNAME
            auto supplierWithCount = lineitemSupplierFilter.join(*agged, *validSupplier);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SIGE(0) || (SIE(0) && SBLE(1));
            };
            TopN topn(100, comparator);
            auto sorted = topn.sort(*supplierWithCount);

            Printer printer(PBEGIN PI(0) PB(1) PEND);
            printer.print(*sorted);
        }
    }
}