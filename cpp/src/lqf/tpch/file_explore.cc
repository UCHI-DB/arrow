//
// Created by Harper on 5/28/20.
//

#include "../threadpool.h"
#include "../data_model.h"
#include "../join.h"
#include "../filter.h"
#include "tpchquery.h"
#include <iostream>

using namespace std;

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::hashcontainer;


int main() {

    auto lineitem = ParquetTable::Open(LineItem::path,
                                       {LineItem::COMMITDATE, LineItem::SHIPDATE, LineItem::RECEIPTDATE});
//    lineitem->blocks()->foreach([](const shared_ptr<Block>& block){
//        cout << block->size() << '\n';
//    });
    auto filter = SboostRow2Filter(LineItem::COMMITDATE, LineItem::RECEIPTDATE, LineItem::SHIPDATE);

    auto filtered = filter.filter(*lineitem);
    cout << filtered->size() << endl;
//    filtered->blocks()->foreach([](const shared_ptr<Block> &block) {
//        auto mblock = dynamic_pointer_cast<MaskedBlock>(block);
//        auto mask = mblock->mask();
//        auto ite = mask->iterator();
//        uint64_t max = -1;
//        auto size = mask->cardinality();
////        cout << size << endl;
//        for (uint32_t i = 0; i < size; ++i) {
//            max = ite->next();
//        }
//        cout << max << '\n';
//    });
}