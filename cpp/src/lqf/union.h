//
// Created by harper on 4/8/20.
//

#ifndef ARROW_UNION_H
#define ARROW_UNION_H

#include <lqf/data_model.h>

namespace lqf {
    class FilterUnion {

    public:
        FilterUnion();

        shared_ptr<Table> execute(initializer_list<Table *>);
    };
}
#endif //ARROW_UNION_H
