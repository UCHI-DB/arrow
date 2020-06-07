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

    auto lineitem = ParquetTable::Open("testres/lineitem",
                                       {LineItem::COMMITDATE, LineItem::SHIPDATE, LineItem::RECEIPTDATE});
    cout << lineitem->numBlocks() << endl;
    cout << lineitem->size() << endl;
    auto blocks = lineitem->blocks()->collect();
    auto block = (*blocks)[0];

}