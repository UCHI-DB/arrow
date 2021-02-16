#include <benchmark/benchmark.h>
#include <cstring>
#include <lqf/data_model.h>
#include <lqf/filter.h>
#include <lqf/join.h>
#include <lqf/filter_executor.h>
#include <lqf/tjoin.h>
#include "tpchquery.h"

class JoinBenchmark : public benchmark::Fixture {
protected:
    uint64_t size_;
public:

    JoinBenchmark() {
    }

    virtual ~JoinBenchmark() {
    }
};

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::sboost;


BENCHMARK_F(JoinBenchmark, LQF)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

        ColFilter custFilter(
                {new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        FilterTJoin<Hash32Predicate> orderOnCustFilterJoin(Orders::CUSTKEY, Customer::CUSTKEY);
        auto joined = orderOnCustFilterJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}

BENCHMARK_F(JoinBenchmark, HashSet)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

        ColFilter custFilter({new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        FilterTJoin<Hash32SetPredicate> orderOnCustFilterJoin(Orders::CUSTKEY, Customer::CUSTKEY);
        auto joined = orderOnCustFilterJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}

BENCHMARK_F(JoinBenchmark, Cuckoo)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
    //run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

        ColFilter custFilter({new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        FilterTJoin<Hash32CuckooPredicate> orderOnCustFilterJoin(Orders::CUSTKEY, Customer::CUSTKEY);
        auto joined = orderOnCustFilterJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}

BENCHMARK_F(JoinBenchmark, Google)(benchmark::State &state) {
    ByteArray segment("HOUSEHOLD");
    for (auto _ : state) {
        // run your benchmark
        auto customerTable = ParquetTable::Open(Customer::path, {Customer::CUSTKEY, Customer::MKTSEGMENT});
        auto orderTable = ParquetTable::Open(Orders::path, {Orders::CUSTKEY});

        ColFilter custFilter({new SboostPredicate<ByteArrayType>(Customer::MKTSEGMENT, bind(&ByteArrayDictEq::build, segment))});
        auto filteredCustTable = custFilter.filter(*customerTable);

        FilterTJoin<Hash32GooglePredicate> orderOnCustFilterJoin(Orders::CUSTKEY, Customer::CUSTKEY);
        auto joined = orderOnCustFilterJoin.join(*orderTable, *filteredCustTable);
        size_ = joined->size();

        FilterExecutor::inst->reset();
    }
}