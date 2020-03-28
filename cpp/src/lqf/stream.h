//
// Created by harper on 2/13/20.
//

#ifndef CHIDATA_STREAM_H
#define CHIDATA_STREAM_H

#include <vector>
#include <memory>
#include <functional>
#include "executor.h"

using namespace std;

namespace lqf {
    namespace stream {
        template<typename OUT>
        class EvalOp {
        public:
            virtual ~EvalOp() {}

            virtual OUT eval() = 0;
        };

        template<typename FROM, typename TO>
        class TransformOp : public EvalOp<TO> {

        public:
            TransformOp(unique_ptr<EvalOp<FROM>> from, function<TO(const FROM &)> f)
                    : previous_(move(from)), mapper_(f) {}

            inline TO eval() override {
                FROM from = previous_->eval();
                return mapper_(from);
            }

        protected:
            unique_ptr<EvalOp<FROM>> previous_;
            function<TO(const FROM &)> mapper_;
        };

        template<typename TYPE>
        class TrivialOp : public EvalOp<TYPE> {
        private:
            TYPE ref_;
        public:
            TrivialOp(const TYPE ref) : ref_(ref) {}

            inline TYPE eval() override {
                return ref_;
            }
        };

    }

    using namespace lqf::stream;

    template<typename VIEW>
    class Stream;

    template<typename FROM, typename TO>
    class StreamLink;

    using namespace lqf::executor;


    class StreamEvaluator {
    public:
        static shared_ptr<Executor> defaultExecutor;

        bool parallel_;

        StreamEvaluator() : parallel_(false) {}

        ~StreamEvaluator() {}

        template<typename T>
        shared_ptr<vector<T>> eval(vector<unique_ptr<EvalOp<T>>> &input) {
            if (parallel_) {
                return evalParallel(input);
            } else {
                return evalSequential(input);
            }
        }

        template<typename T>
        inline shared_ptr<vector<T>> evalParallel(vector<unique_ptr<EvalOp<T>>> &input) {
            vector<function<T()>> tasks;
            for (auto &eval:input) {
                auto op = eval.get();
                tasks.push_back([=]() {
                    return op->eval();
                });
            }
            shared_ptr<vector<T>> res = move(defaultExecutor->invokeAll(tasks));
            return res;
        }

        template<typename T>
        inline shared_ptr<vector<T>> evalSequential(vector<unique_ptr<EvalOp<T>>> &input) {
            auto result = make_shared<vector<T>>();
            for (auto &eval:input) {
                result->push_back(eval->eval());
            }

            return result;
        }
    };

    template<typename VIEW>
    class Stream : public enable_shared_from_this<Stream<VIEW>> {
    public:
        Stream() : evaluator_(nullptr) {}

        template<typename NEXT>
        shared_ptr<Stream<NEXT>> map(function<NEXT(const VIEW &)> f) {
            return make_shared<StreamLink<VIEW, NEXT>>(this->shared_from_this(), f, evaluator_);
        }

        /// Use int32_t as workaround as void cannot be allocated return space
        void foreach(function<void(const VIEW &)> f) {
            vector<unique_ptr<EvalOp<int32_t>>> holder;
            while (!isEmpty()) {
                holder.push_back(unique_ptr<EvalOp<int32_t>>(
                        new TransformOp<VIEW, int32_t>(
                                next(),
                                [=](const VIEW &a) {
                                    f(a);
                                    return 0;
                                })));
            }
            evaluator_->eval(holder);
        }

        shared_ptr<vector<VIEW>> collect() {
            vector<unique_ptr<EvalOp<VIEW>>> holder;
            while (!isEmpty()) {
                holder.push_back(next());
            }
            auto result = evaluator_->eval(holder);
            return result;
        }

        VIEW reduce(function<VIEW(VIEW &, VIEW &)> reducer) {
            auto collected = collect();
            // TODO it is possible to execute reduce in parallel
            if (collected->size() == 1) {
                return move((*collected)[0]);
            }
            VIEW first = move((*collected)[0]);
            auto ite = collected->begin();
            ite++;
            for (; ite != collected->end(); ite++) {
                first = reducer(first, *ite);
            }
            return first;
        }

        inline bool isParallel() {
            return evaluator_->parallel_;
        }

        inline shared_ptr<Stream<VIEW>> parallel() {
            evaluator_->parallel_ = true;
            return this->shared_from_this();
        }

        inline shared_ptr<Stream<VIEW>> sequential() {
            evaluator_->parallel_ = false;
            return this;
        }

        virtual bool isEmpty() = 0;

        virtual unique_ptr<EvalOp<VIEW>> next() = 0;

    protected:
        shared_ptr<StreamEvaluator> evaluator_;
    };

    template<typename FROM, typename TO>
    class StreamLink : public Stream<TO> {
    public:
        StreamLink(shared_ptr<Stream<FROM>> source, function<TO(const FROM &)> mapper, shared_ptr<StreamEvaluator> eval)
                : source_(source), mapper_(mapper) {
            this->evaluator_ = eval;
        }

        bool isEmpty() override {
            return source_->isEmpty();
        }

        unique_ptr<EvalOp<TO>> next() override {
            return unique_ptr<TransformOp<FROM, TO>>(new TransformOp<FROM, TO>(source_->next(), mapper_));
        }

    protected:
        shared_ptr<Stream<FROM>> source_;
        function<TO(const FROM &)> mapper_;
    };

    template<typename TYPE>
    class VectorStream : public Stream<TYPE> {
    private:
        const vector<TYPE> &data_;
        typename vector<TYPE>::const_iterator position_;
    public:
        VectorStream(const vector<TYPE> &data) :
                data_(data), position_(data.begin()) {
            this->evaluator_ = make_shared<StreamEvaluator>();
        }

        virtual ~VectorStream() {}

        bool isEmpty() override {
            return position_ == data_.end();
        }

        unique_ptr<EvalOp<TYPE>> next() override {
            return unique_ptr<EvalOp<TYPE>>(new TrivialOp<TYPE>(*(position_++)));
        };
    };

    class IntStream : public Stream<int32_t> {

    public:
        static shared_ptr<IntStream> Make(int32_t from, int32_t to) {
            return make_shared<IntStream>(from, to, 1);
        }

        IntStream(int32_t from, int32_t to, int32_t step = 1)
                : to_(to), step_(step), pointer_(from) {
            this->evaluator_ = make_shared<StreamEvaluator>();
        }

        bool isEmpty() override {
            return pointer_ >= to_;
        }

        unique_ptr<EvalOp<int32_t>> next() override {
            int32_t value = pointer_;
            pointer_ += step_;
            return unique_ptr<TrivialOp<int32_t>>(new TrivialOp<int32_t>(*(new int32_t(value))));
        }

    private:
        int32_t to_;
        int32_t step_;
        int32_t pointer_;
    };

}
#endif //CHIDATA_STREAM_H
