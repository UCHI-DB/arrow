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
        namespace q21 {
            class ItemJoin : public Join {
            protected:
                unordered_set<int32_t> denied_;
                unordered_map<int32_t, pair<int32_t, int32_t>> container_;
                mutex map_lock_;
                unordered_map<int32_t, mutex> order_locks_;

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
                                for (int i = 0; i < count; ++i) {
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

            using namespace sboost;


            ColFilter orderFilter(new SboostPredicate<ByteArrayType>(Orders::ORDERSTATUS,
                                                                     bind(&ByteArrayDictEq::build, status)));
            auto validorder = orderFilter.filter(*order);

            RowFilter linedateFilter([](DataRow &row) {
                return row[LineItem::RECEIPTDATE].asByteArray() > row[LineItem::COMMITDATE].asByteArray();
            });
            auto l1 = linedateFilter.filter(*lineitem);

            HashFilterJoin lineWithOrderJoin(LineItem::ORDERKEY, Orders::ORDERKEY);
            l1 = lineWithOrderJoin.join(*l1, *validorder);

            FilterMat mat;
            auto matl1 = mat.mat(*l1);

            ItemJoin itemExistJoin;
            // ORDERKEY, SUPPKEY
            l1 = itemExistJoin.join(*lineitem, *l1);

            using namespace agg;
            HashAgg countAgg(vector<uint32_t>{1, 1}, {AGI(1)}, []() { return vector<AggField *>{new Count()}; },
                             COL_HASHER(1));
            countAgg.useVertical();
            auto agged = countAgg.agg(*l1);

            ColFilter supplierNationFilter(new SboostPredicate<Int32Type>(Supplier::NATIONKEY,
                                                                          bind(&Int32DictEq::build, 3)));
            auto validSupplier = supplierNationFilter.filter(*supplier);

            HashJoin lineitemSupplierFilter(0, Supplier::SUPPKEY, new RowBuilder({JL(1), JRS(Supplier::NAME)}));
            auto supplierWithCount = lineitemSupplierFilter.join(*agged, *validSupplier);

            function<bool(DataRow *, DataRow *)> comparator = [](DataRow *a, DataRow *b) {
                return SIGE(1) || (SIE(1) && SBLE(1));
            };
            TopN topn(100, comparator);
            auto sorted = topn.sort(*supplierWithCount);

            auto printer = Printer::Make(PBEGIN PI(0) PB(1) PEND);
            printer->print(*sorted);
        }
    }
}