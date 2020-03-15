//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_AGG_H
#define LQF_OPERATOR_AGG_H

#include "data_model.h"

using namespace std;
using namespace std::placeholders;
namespace lqf {

    class AggField {
    protected:
        uint32_t readIndex_;
    public:
        AggField(uint32_t readIndex);

        virtual void reduce(DataRow &) = 0;

        virtual void dump(DataRow &, uint32_t) = 0;

        virtual void merge(AggField &) = 0;
    };

    namespace agg {

        struct AsDouble {
            static double get(DataField &df) { return df.asDouble(); }
        };

        struct AsInt {
            static int32_t get(DataField &df) { return df.asInt(); }
        };

        template<typename T, typename ACC>
        class Sum : public AggField {
        protected:
            T value_;
        public:
            Sum(uint32_t index);

            virtual void reduce(DataRow &);

            virtual void dump(DataRow &, uint32_t);

            virtual void merge(AggField &);
        };

        using IntSum = Sum<int32_t, AsInt>;
        using DoubleSum = Sum<double, AsDouble>;

        class Count : public AggField {
        protected:
            int32_t count_;
        public:
            Count();

            virtual void reduce(DataRow &);

            virtual void dump(DataRow &, uint32_t);

            virtual void merge(AggField &);
        };

        template<typename T, typename ACC>
        class Max : public AggField {
        protected:
            T value_;
        public:
            Max(uint32_t rIndex);

            virtual void reduce(DataRow &);

            virtual void dump(DataRow &, uint32_t);

            virtual void merge(AggField &);
        };

        using IntMax = Max<int32_t, AsInt>;
        using DoubleMax = Max<double, AsDouble>;

        template<typename T, typename ACC>
        class Avg : public AggField {
        protected:
            T value_;
            uint32_t count_;
        public:
            Avg(uint32_t rIndex);

            virtual void reduce(DataRow &);

            virtual void dump(DataRow &, uint32_t);

            virtual void merge(AggField &);
        };

        using IntAvg = Avg<int32_t, AsInt>;
        using DoubleAvg = Avg<double, AsDouble>;
    }

    class AggReducer {
    private:
        MemDataRow header_;
        vector<unique_ptr<AggField>> fields_;
    public:
        AggReducer(uint32_t numHeaders, initializer_list<AggField *> fields);

        MemDataRow &header();

        void reduce(DataRow &);

        void dump(DataRow &);

        void merge(AggReducer &reducer);
    };

    template<typename CORE>
    class Agg {
    protected:
        function<shared_ptr<CORE>()> coreMaker_;

        shared_ptr<CORE> processBlock(const shared_ptr<Block> &block) {
            auto rows = block->rows();
            auto core = coreMaker_();
            uint64_t blockSize = block->size();
            for (uint32_t i = 0; i < blockSize; ++i) {
                core->consume(rows->next());
            }
            return core;
        }

    public:
        Agg(function<shared_ptr<CORE>()> coreMaker) : coreMaker_(coreMaker) {}

        shared_ptr<Table> agg(Table &input) {
            function<shared_ptr<CORE>(
                    const shared_ptr<Block> &)> mapper = bind(&Agg::processBlock, this, _1);

            function<shared_ptr<CORE>(const shared_ptr<CORE> &, const shared_ptr<CORE> &)> reducer =
                    [](const shared_ptr<CORE> &a, const shared_ptr<CORE> &b) {
                        a->reduce(*b);
                        return a;
                    };
            auto merged = input.blocks()->map(mapper)->reduce(reducer);

            shared_ptr<MemTable> result = MemTable::Make(merged->numFields());
            shared_ptr<MemBlock> block = result->allocate(merged->size());
            merged->dump(*block);

            return result;
        }
    };

    class HashCore {
    private:
        uint32_t numFields_;
        unordered_map<uint64_t, unique_ptr<AggReducer>> container_;
        function<uint64_t(DataRow & )> hasher_;
        function<unique_ptr<AggReducer>(DataRow & )> headerInit_;
    public:
        HashCore(uint32_t numFields, function<uint64_t(DataRow & )> hasher,
                 function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        virtual ~HashCore();

        uint32_t size();

        uint32_t numFields();

        void consume(DataRow &row);

        void reduce(HashCore &another);

        void dump(MemBlock &block);
    };

    using HashAgg = Agg<HashCore>;

    class TableCore {
    private:
        uint32_t numFields_;
        vector<unique_ptr<AggReducer>> container_;
        function<uint32_t(DataRow & )> indexer_;
        function<unique_ptr<AggReducer>(DataRow & )> headerInit_;
    public:
        TableCore(uint32_t numFields, uint32_t tableSize, function<uint32_t(DataRow & )> indexer,
                  function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        virtual ~TableCore();

        uint32_t size();

        uint32_t numFields();

        void consume(DataRow &row);

        void reduce(TableCore &another);

        void dump(MemBlock &block);
    };

    using TableAgg = Agg<TableCore>;


    class SimpleCore {
    private:
        uint32_t numFields_;
        function<unique_ptr<AggReducer>(DataRow & )> headerInit_;
        unique_ptr<AggReducer> reducer_;
    public:
        SimpleCore(uint32_t numFields, function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        uint32_t size();

        uint32_t numFields();

        void consume(DataRow &row);

        void reduce(SimpleCore &another);

        void dump(MemBlock &block);
    };

    using SimpleAgg = Agg<SimpleCore>;
}
#endif //LQF_OPERATOR_AGG_H
