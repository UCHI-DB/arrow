//
// Created by Harper on 5/28/20.
//

#include "../threadpool.h"
#include "../data_model.h"
#include "../join.h"
#include "../tjoin.h"
#include "../filter.h"
#include "../agg.h"
#include "../filter_executor.h"
#include "tpchquery.h"
#include <iostream>
#include <memory>
#include <chrono>
#include <mutex>

using namespace std;
using namespace std::chrono;

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::sboost;

int main() {
    ByteArray segment("HOUSEHOLD");
    //run your benchmark
    auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
    auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

    ColFilter custFilter(
            {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
    auto filteredCustTable = custFilter.filter(*customerTable);

    HashTJoin<Hash32MapHeapContainer> orderOnCustJoin(
            Orders::CUSTKEY, Customer::CUSTKEY, new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)},
                                                               false, true));

    auto joined = orderOnCustJoin.join(*orderTable, *filteredCustTable);
    cout << joined->size() << '\n';


    FilterExecutor::inst->reset();
}