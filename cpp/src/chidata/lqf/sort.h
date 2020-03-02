//
// Created by harper on 2/27/20.
//

#ifndef ARROW_SORT_H
#define ARROW_SORT_H

#include <chidata/heap.h>
#include <chidata/lqf/data_model.h>

using namespace std;

namespace chidata {
    namespace lqf {

        class TopN {
        private:
            uint32_t n_;
            uint32_t numFields_;
            function<int32_t(DataRow*, DataRow*)> comparator_;
            Heap<DataRow*> heap_;
        public:
            TopN(uint32_t, uint32_t, function<int32_t(DataRow*, DataRow*)>);

            shared_ptr<Table> sort(Table &);

        protected:
            void sortBlock(shared_ptr<Block> &input);
        };
    }
}

#endif //ARROW_SORT_H
