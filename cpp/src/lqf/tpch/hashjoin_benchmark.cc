#include <benchmark/benchmark.h>
#include <cstring>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/filter_executor.h>
#include <lqf/tjoin.h>
#include "tpchquery.h"

class HashJoinBenchmark : public benchmark::Fixture {
protected:
    uint64_t size_;
public:

    HashJoinBenchmark() {
    }

    virtual ~HashJoinBenchmark() {
    }
};

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::sboost;

//BENCHMARK_F(JoinBenchmark, Filter)(benchmark::State &state) {
//    ByteArray segment("HOUSEHOLD");
//    for (auto _ : state) {
//        //run your benchmark
//        auto customerTable = ParquetTable::Open(Customer::path, {Customer::MKTSEGMENT,Customer::CUSTKEY});
//
//        ColFilter custFilter(
//                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
//        auto filteredCustTable = custFilter.filter(*customerTable);
//        size_ = filteredCustTable->size();
//        FilterExecutor::inst->reset();
//    }
//}

BENCHMARK_F(HashJoinBenchmark, LQF)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path,
                                                {Customer::CUSTKEY, Customer::MKTSEGMENT, Customer::NATIONKEY});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY});

        ColFilter custFilter(
                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        HashTJoin<Hash32SparseContainer> orderOnCustJoin(
                Orders::CUSTKEY, Customer::CUSTKEY, new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)},
                                                                   false, true));

        auto joined = orderOnCustJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}

BENCHMARK_F(HashJoinBenchmark, Google)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path,
                                        {Customer::CUSTKEY, Customer::MKTSEGMENT, Customer::NATIONKEY});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY, Orders::ORDERKEY});

        ColFilter custFilter(
            {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        HashTJoin<Hash32MapHeapContainer> orderOnCustJoin(Orders::CUSTKEY, Customer::CUSTKEY, new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)},
                                                           false, true));

        auto joined = orderOnCustJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}

//BENCHMARK_F(JoinBenchmark, Cuckoo)(benchmark::State &state) {
//    ByteArray segment("HOUSEHOLD");
//    for (auto _ : state) {
//    //run your benchmark
//        auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
//        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});
//
//        ColFilter custFilter({new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
//        auto filteredCustTable = custFilter.filter(*customerTable);
//
//HashTJoin<Hash32SparseContainer> orderOnCustJoin(
//        Orders::CUSTKEY, Customer::CUSTKEY, new RowBuilder({JL(Orders::ORDERKEY), JR(Customer::NATIONKEY)},
//                                                           false, true));
//
//auto joined = orderOnCustJoin.join(*orderTable, *filteredCustTable);
//size_ = joined->size();

//
//        FilterExecutor::inst->reset();
//    }
//}

