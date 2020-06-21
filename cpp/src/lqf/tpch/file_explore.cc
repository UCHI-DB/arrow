//
// Created by Harper on 5/28/20.
//

#include "../threadpool.h"
#include "../data_model.h"
#include "../join.h"
#include "../filter.h"
#include "tpchquery.h"
#include <iostream>
#include <memory>
#include <mutex>

using namespace std;

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::hashcontainer;


class TestBuffer {
public:
    thread_local static uint32_t index_;
    thread_local static uint32_t offset_;
    thread_local static bool ready_;

    vector<shared_ptr<vector<uint64_t>>> memory_;
    mutex lock_;

    void init() {
        if (!ready_) {
            new_slab();
            ready_ = true;
        }
    }

    void new_slab() {
        std::lock_guard<mutex> lock(lock_);
        index_ = memory_.size();
        memory_.push_back(make_shared<vector<uint64_t>>(1000));
        offset_ = 0;
    }

    void put(uint64_t value) {
        init();
        if (offset_ == 1000) {
            new_slab();
        }
        if(memory_[index_] == nullptr) {
            cout << "Error" << endl;
        }
        *(memory_[index_]->data() + offset_) = value;
        offset_++;
    }
};

thread_local uint32_t TestBuffer::index_ = 0;
thread_local uint32_t TestBuffer::offset_ = 1000;
thread_local bool TestBuffer::ready_ = false;

int main() {
    TestBuffer buffer;
    vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        thread t = thread([&buffer, i]() {
            for (int j = 0; j < 10000; ++j) {
                buffer.put(i * 10000 + j);
            }
        });
        threads.emplace_back(move(t));
    }
    for (auto &t: threads) {
        t.join();
    }
}