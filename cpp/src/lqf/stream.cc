//
// Created by harper on 3/14/20.
//

#include "stream.h"

namespace lqf {

    using namespace arrow::internal;
//    shared_ptr<Executor> StreamEvaluator::defaultExecutor = Executor::Make(30);
    shared_ptr<ThreadPool> StreamEvaluator::defaultExecutor = *(ThreadPool::Make(25));
}