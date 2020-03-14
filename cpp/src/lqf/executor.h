//
// Created by harper on 3/14/20.
//

#ifndef ARROW_EXECUTOR_H
#define ARROW_EXECUTOR_H

#include<thread>
#include<memory>
#include<queue>
#include<vector>
#include<mutex>
#include<functional>
#include <condition_variable>

using namespace std;

namespace lqf {
    namespace executor {

        class Semaphore {
        private:
            std::mutex mutex_;
            std::condition_variable condition_;
            unsigned long count_ = 0; // Initialized as locked.

        public:
            void notify() {
                std::lock_guard<decltype(mutex_)> lock(mutex_);
                ++count_;
                condition_.notify_one();
            }

            void wait() {
                std::unique_lock<decltype(mutex_)> lock(mutex_);
                while (!count_) // Handle spurious wake-ups.
                    condition_.wait(lock);
                --count_;
            }

            bool try_wait() {
                std::lock_guard<decltype(mutex_)> lock(mutex_);
                if (count_) {
                    --count_;
                    return true;
                }
                return false;
            }
        };

        class Executor;

        class Future {
        public:
            virtual void wait() = 0;

            virtual bool isDone() = 0;
        };

        template<typename T>
        class CallFuture : public virtual Future {
        public:
            virtual T get() = 0;

        };

        class Task : public virtual Future {
            friend Executor;
        protected:
            unique_ptr<Semaphore> signal_;

            function<void()> runnable_;
        public:
            Task(function<void()> runnable)
                    : signal_(unique_ptr<Semaphore>(new Semaphore())),
                      runnable_(runnable) {}

            virtual void wait() override {
                signal_->wait();
            }

            virtual bool isDone() override {
                return signal_->try_wait();
            }

            inline void run() {
                runnable_();
            }
        };

        template<typename T>
        class CallTask : public virtual CallFuture<T>, public virtual Task {
        public:
            CallTask(function<T()> callable)
                    : Task(bind(&CallTask::call, this)), callable_(callable) {}

        protected:
            T result;
            function<T()> callable_;

            void call() {
                result = callable_();
            }

            T get() override {
                this->wait();
                return move(result);
            }
        };

        class Executor {
        protected:
            bool shutdown_;
            uint32_t pool_size_;
            vector<unique_ptr<thread>> threads_;
            Semaphore has_task_;
            mutex fetch_task_;
            queue<shared_ptr<Task>> tasks_;

            void submit(shared_ptr<Task>);

        public:

            virtual ~Executor();

            void shutdown();

            shared_ptr<Future> submit(function<void()>);

            template<typename T>
            shared_ptr<CallFuture<T>> submit(function<T()> task) {
                auto t = make_shared<CallTask<T>>(task);
                submit(t);
                return t;
            }

            template<typename T>
            unique_ptr<vector<T>> invokeAll(vector<function<T()>>& tasks) {
                vector<shared_ptr<CallFuture<T>>> futures;
                for (auto t: tasks) {
                    auto res = make_shared<CallTask<T>>(t);
                    submit(res);
                    futures.push_back(res);
                }
                unique_ptr<vector<T>> result = unique_ptr<vector<T>>();
                for (auto future: futures) {
                    future->wait();
                    result->push_back(future->get());
                }
                return result;
            }

            static shared_ptr<Executor> Make(uint32_t psize);

            Executor(uint32_t pool_size);

        protected:
            void routine();
        };


    }
}
#endif //ARROW_EXECUTOR_H
