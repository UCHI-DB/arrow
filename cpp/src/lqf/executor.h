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
#include<future>
#include<functional>
#include <condition_variable>

using namespace std;

namespace lqf {
    namespace executor {

        class Semaphore {
        private:
            std::mutex mutex_;
            std::condition_variable condition_;
            uint32_t count_ = 0; // Initialized as locked.

        public:
            void notify() {
                std::lock_guard<decltype(mutex_)> lock(mutex_);
                ++count_;
                condition_.notify_one();
            }

            void notify(uint32_t num) {
                std::lock_guard<decltype(mutex_)> lock(mutex_);
                count_ += num;
                for (uint32_t i = 0; i < num; ++i) {
                    condition_.notify_one();
                }
            }

            void wait() {
                std::unique_lock<decltype(mutex_)> lock(mutex_);
                while (!count_) // Handle spurious wake-ups.
                    condition_.wait(lock);
                --count_;
            }

            void wait(uint32_t num) {
                std::unique_lock<decltype(mutex_)> lock(mutex_);
                uint32_t remain = num;
                while (remain) {
                    while (!count_) {
                        condition_.wait(lock);
                    }
                    auto delta = std::min(remain, count_);
                    remain -= delta;
                    count_ -= delta;
                }
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

        class Task {
            friend Executor;
        protected:
            function<void()> runnable_;
        public:
            Task(function<void()> runnable) : runnable_(runnable) {}

            inline void run() {
                runnable_();
            }
        };

        template<typename T>
        class CallTask : public Task {
            friend Executor;
        private:
            promise<T> promise_;
        public:
            CallTask(function<T()> callable)
                    : Task(bind(&CallTask::call, this)), callable_(callable) {}

        protected:
            function<T()> callable_;

            void call() {
                promise_.set_value(callable_());
            }

            inline future<T> getFuture() {
                return promise_.get_future();
            }
        };

        class Executor {
        protected:
            bool shutdown_;
            Semaphore shutdown_guard_;
            uint32_t pool_size_;
            vector<unique_ptr<thread>> threads_;
            Semaphore has_task_;
            mutex fetch_task_;
            queue<unique_ptr<Task>> tasks_;

            void submit(unique_ptr<Task>);

        public:

            virtual ~Executor();

            void shutdown();

            void submit(function<void()>);

            template<typename T>
            future<T> submit(function<T()> task) {
                auto t = new CallTask<T>(task);
                submit(unique_ptr<CallTask<T>>(t));
                return t->getFuture();
            }

            template<typename T>
            unique_ptr<vector<T>> invokeAll(vector<function<T()>> &tasks) {
                vector<future<T>> futures;
                for (auto t: tasks) {
                    auto res = new CallTask<T>(t);
                    submit(unique_ptr<CallTask<T>>(res));
                    futures.push_back(res->getFuture());
                }
                unique_ptr<vector<T>> result = unique_ptr<vector<T>>(new vector<T>());
                for (auto ite = futures.begin(); ite != futures.end(); ite++) {
                    (*ite).wait();
                    result->push_back((*ite).get());
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
