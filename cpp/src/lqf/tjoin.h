//
// Support choosing hash container for each join operator via template
// Created by harper on 1/24/21.
//

#ifndef LQF_TJOIN_H
#define LQF_TJOIN_H

#include "join.h"

namespace lqf {

    template<typename Container>
    class HashBasedTJoin : public Join {
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

    template<typename Container>
    class HashTJoin : public HashBasedTJoin<Container> {
    public:
        HashTJoin(uint32_t, uint32_t, RowBuilder *, function<bool(DataRow &, DataRow &)> pred = nullptr,
                  uint32_t expect_size = CONTAINER_SIZE);

        virtual ~HashTJoin() = default;

    protected:
        RowBuilder *rowBuilder_;
        function<bool(DataRow &, DataRow &)> predicate_;

        virtual shared_ptr<Block> probe(const shared_ptr<Block> &) override;
    };

    template<typename Container>
    class HashColumnTJoin : public HashBasedTJoin<Container> {
    public:
        HashColumnTJoin(uint32_t, uint32_t, ColumnBuilder *, bool need_filter = false,
                        uint32_t expect_size = CONTAINER_SIZE);

        virtual ~HashColumnTJoin() = default;

    protected:
        bool need_filter_;

        ColumnBuilder *columnBuilder_;

        shared_ptr<Block> probe(const shared_ptr<Block> &) override;
    };

    /**
     * ParquetHashColumnJoin working on ParquetTable, caching columns into MemvTable
     * TODO ParquetHashColumnJoin does not support copying raw data into memory now. Need support later
     */

    template<typename Container>
    class ParquetHashColumnTJoin : public HashColumnTJoin<Container> {
    public:
        ParquetHashColumnTJoin(uint32_t, uint32_t, ColumnBuilder *, uint32_t expect_size = CONTAINER_SIZE);

        virtual ~ParquetHashColumnTJoin() = default;

    protected:
        shared_ptr<Block> probe(const shared_ptr<Block> &) override;
    };

    template <typename Container>
    class FilterTJoin : public Join {
    protected:
        uint32_t left_key_index_;
        uint32_t right_key_index_;
        uint32_t expect_size_;
        shared_ptr<Container> predicate_;
        bool anti_ = false;
        bool useBitmap_;
    public:
        FilterTJoin(uint32_t, uint32_t, uint32_t expect_size = CONTAINER_SIZE);

        virtual ~FilterTJoin() = default;

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        void useAnti() { anti_ = true; }

    protected:
        virtual shared_ptr<Block> probe(const shared_ptr<Block> &);
    };
}

#endif //LQF_TJOIN_H
