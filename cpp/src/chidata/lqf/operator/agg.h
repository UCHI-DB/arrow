//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_AGG_H
#define LQF_OPERATOR_AGG_H

#include <chidata/lqf/data_model.h>

using namespace std;
using namespace std::placeholders;
namespace chidata {
    namespace lqf {
        namespace opr {

            class AggField {
            protected:
                uint32_t readIndex_;
                uint32_t writeIndex_;
            public:
                AggField(uint32_t readIndex, uint32_t writeIndex);

                virtual void reduce(DataRow &) = 0;

                virtual void dump(DataRow &) = 0;

                virtual void merge(AggField &) = 0;
            };

            namespace aggfield {
                class DoubleSum : public AggField {
                protected:
                    double value_;
                public:
                    DoubleSum(uint32_t rIndex, uint32_t wIndex);

                    virtual void reduce(DataRow &);

                    virtual void dump(DataRow &);

                    virtual void merge(AggField &);
                };

                class Count : public AggField {
                protected:
                    int32_t count_;
                public:
                    Count(uint32_t rIndex, uint32_t wIndex);

                    virtual void reduce(DataRow &);

                    virtual void dump(DataRow &);

                    virtual void merge(AggField &);
                };
            }

            class AggReducer {
            private:
                vector<unique_ptr<AggField>> fields_;
            public:
                AggReducer(initializer_list<unique_ptr<AggField>> fields);

                void reduce(DataRow &);

                void dump(DataRow &);

                void merge(AggReducer &reducer);
            };

            template<typename CORE>
            class Agg {
            protected:
                function<shared_ptr<CORE>()> coreMaker_;

                shared_ptr<CORE> processBlock(Block &block) {
                    auto rows = block.rows();
                    auto core = coreMaker_();
                    for (int i = 0; i < block.size(); ++i) {
                        core->consume(rows->next());
                    }
                    return core;
                }

            public:
                Agg(function<shared_ptr<CORE>()> coreMaker) : coreMaker_(coreMaker) {}

                shared_ptr<Table> agg(Table &input) {
                    function<shared_ptr<CORE>(Block &)> mapper = bind(&Agg::processBlock, this, _1);

                    function<shared_ptr<CORE>(shared_ptr<CORE>, shared_ptr<CORE>)> reducer =
                            [](shared_ptr<CORE> a, shared_ptr<CORE> b) {
                                a->reduce(*b);
                                return a;
                            };
                    CORE merged = input.blocks()->map(mapper)->reduce(reducer);

                    shared_ptr<MemTable> result = make_shared(merged.numFields());
                    shared_ptr<MemBlock> block = result->allocate(merged.size());
                    merged.dump(*block);

                    return result;
                }
            };

            class HashCore {
            private:
                uint32_t numFields_;
                unordered_map<uint64_t, unique_ptr<AggReducer>> container_;
                function<uint64_t(DataRow &)> hasher_;
                function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
            public:
                HashCore(uint32_t numFields, function<uint64_t(DataRow &)> hasher,
                         function<unique_ptr<AggReducer>(DataRow &)> headerInit);

                virtual ~HashCore();

                uint32_t size();

                uint32_t numFields();

                void consume(DataRow &row);

                void reduce(HashCore &another);

                void dump(MemBlock &block);
            };

            using HashAgg = Agg<HashCore>;

            class SimpleCore {
            private:
                uint32_t numFields_;
                function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
                unique_ptr<AggReducer> reducer_;
            public:
                SimpleCore(uint32_t numFields, function<unique_ptr<AggReducer>(DataRow &)> headerInit);

                uint32_t size();

                uint32_t numFields();

                void consume(DataRow &row);

                void reduce(SimpleCore &another);

                void dump(MemBlock &block);
            };

            using SimpleAgg = Agg<SimpleCore>;
        }
    }
}
#endif //LQF_OPERATOR_AGG_H
