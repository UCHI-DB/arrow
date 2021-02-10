//
// Created by harper on 1/10/21.
//

#ifndef LQF_ENV_H
#define LQF_ENV_H

#include "filter_executor.h"
#include "stat.h"

namespace lqf {
    /**
     * This is a unified place to execute initialization code prior to each query
     */

    void env_init() {
        stat::MemEstimator::INST.Reset();
    }

    void env_cleanup() {
        FilterExecutor::inst->reset();
    }
}

#endif //LQF_ENV_H
