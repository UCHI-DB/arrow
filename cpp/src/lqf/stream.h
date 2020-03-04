//
// Created by harper on 2/13/20.
//

#ifndef CHIDATA_STREAM_H
#define CHIDATA_STREAM_H

#include <vector>
#include <functional>
#include <memory>

using namespace std;

namespace lqf {
    template<typename FROM, typename TO>
    class StreamChain;

    template<typename TYPE>
    class Stream : public enable_shared_from_this<Stream<TYPE>> {
    public:
        template<typename NEXT>
        shared_ptr<Stream<NEXT>> map(function<NEXT(const TYPE &)> f) {
            return make_shared<StreamChain<TYPE, NEXT>>(this->shared_from_this(), f);
        }

        void foreach(function<void(TYPE &)> f) {
            while (!isEmpty()) {
                TYPE n = next();
                f(n);
            }
        }

        shared_ptr<vector<TYPE>> collect() {
            shared_ptr<vector<TYPE>> buffer = make_shared<vector<TYPE>>();
            while (!isEmpty()) {
                buffer->push_back(move(next()));
            }
            return buffer;
        }

        TYPE reduce(function<TYPE(TYPE &, TYPE &)> reducer) {
            TYPE current = next();
            while (!isEmpty()) {
                TYPE n = next();
                current = reducer(current, n);
            }
            return current;
        }

        virtual bool isEmpty() = 0;

        virtual TYPE next() = 0;

    };

    template<typename TYPE>
    class VectorStream : public virtual Stream<TYPE> {
    private:
        const vector<TYPE> &data_;
        typename vector<TYPE>::const_iterator position_;
    public:
        VectorStream(const vector<TYPE> &data) : data_(data), position_(data.begin()) {}

        virtual ~VectorStream() {}

        virtual bool isEmpty() override {
            return position_ == data_.end();
        }

        virtual TYPE next() override {
            auto value = *position_;
            position_++;
            return value;
        }
    };

    class IntStream : public Stream<int32_t> {

    public:
        static shared_ptr<IntStream> Make(int32_t from, int32_t to) {
            return make_shared<IntStream>(from, to, 1);
        }

        IntStream(int32_t from, int32_t to, int32_t step = 1) : to_(to), step_(step), pointer_(from) {}

        virtual bool isEmpty() override {
            return pointer_ >= to_;
        }

        virtual int32_t next() override {
            int32_t value = pointer_;
            pointer_ += step_;
            return value;
        }

    private:
        int32_t to_;
        int32_t step_;
        int32_t pointer_;
    };

    template<typename FROM, typename TO>
    class StreamChain : public Stream<TO> {

    public:
        StreamChain(shared_ptr<Stream<FROM>> from, function<TO(const FROM &)> f) {
            this->previous = from;
            this->mapper = f;
        }

        virtual bool isEmpty() override {
            return this->previous->isEmpty();
        }

        virtual TO next() override {
            return this->mapper(move(previous->next()));
        }

    protected:
        shared_ptr<Stream<FROM>> previous;
        function<TO(const FROM &)> mapper;
    };


}
#endif //CHIDATA_STREAM_H
