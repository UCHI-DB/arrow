//
// Support choosing hash container for each join operator via template
// Created by harper on 1/24/21.
//

#ifndef LQF_TJOIN_H
#define LQF_TJOIN_H

#include "join.h"

namespace lqf {

    template<typename Container>
    class HashBasedTJoin : public HashBasedJoin {
    protected:
        uint32_t left_key_index_;
        uint32_t right_key_index_;
        unique_ptr<JoinBuilder> builder_;
        shared_ptr<Container> container_;
        bool outer_ = false;
        uint32_t expect_size_;
    public:
        HashBasedTJoin(uint32_t, uint32_t, JoinBuilder *builder,
                       uint32_t expect_size = CONTAINER_SIZE);

        virtual ~HashBasedTJoin() = default;

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        inline void useOuter() { outer_ = true; };
    protected:
        virtual shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock) = 0;

        shared_ptr<Block> makeBlock(uint32_t);

        shared_ptr<TableView> makeTable(unique_ptr<Stream<shared_ptr<Block>>>);
    };
}

#endif //LQF_TJOIN_H
