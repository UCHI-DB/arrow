//
// Created by Harper on 5/28/20.
//

#include "../threadpool.h"
#include "../data_model.h"
#include "../join.h"
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
//    ByteArray segment("HOUSEHOLD");
//    auto start = high_resolution_clock::now();
//
//    for (int i = 0; i < 10; ++i) {
//        //run your benchmark
//        auto customerTable = ParquetTable::Open(Customer::path, {Customer::MKTSEGMENT, Customer::CUSTKEY});
//
//        ColFilter custFilter(
//                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
//        auto filteredCustTable = custFilter.filter(*customerTable);
//        cout << filteredCustTable->size() << '\n';
//        FilterExecutor::inst->reset();
//    }
//    // Get ending timepoint
//    auto stop = high_resolution_clock::now();
//    auto duration = duration_cast<microseconds>(stop - start);
//    cout << "Time taken: " << duration.count() / 10 << " microseconds" << endl;

    auto table = ParquetTable::Open(Orders::path,{Orders::CUSTKEY});
    StripeHashAgg agg(16, COL_HASHER(Orders::CUSTKEY), COL_HASHER(0),
                      RowCopyFactory().field(F_REGULAR, Orders::CUSTKEY, 0)->buildSnapshot(),
                      RowCopyFactory().field(F_REGULAR, 0, 0)->buildSnapshot(),
                      []() { return vector<AggField *>{new Count()}; });
    auto result = agg.agg(*table);
    cout << result->size() << '\n';
}