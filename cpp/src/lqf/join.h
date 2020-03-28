//
// Created by harper on 2/25/20.
//

#ifndef CHIDATA_LQF_JOIN_H
#define CHIDATA_LQF_JOIN_H

#include <climits>
#include "data_model.h"

namespace lqf {
    using namespace std;

    static function<bool(DataRow &, DataRow &)> TRUE = [](DataRow &a, DataRow &b) { return true; };

    namespace join {

        class RowBuilder {
            vector<uint32_t> left_;
            vector<uint32_t> right_;
            bool needkey_;
        public:
            RowBuilder(initializer_list<uint32_t>, initializer_list<uint32_t>, bool needkey = false);

            shared_ptr<MemDataRow> snapshot(DataRow &);

            uint32_t hashSize();

            uint32_t outputSize();

            void build(DataRow &, DataRow &, DataRow &, int32_t key);
        };

        class IntPredicate {
        public:
            virtual bool test(uint32_t) = 0;
        };

        class HashPredicate : public IntPredicate {
        private:
            unordered_set<uint32_t> content_;
        public:
            HashPredicate();

            void add(uint32_t);

            bool test(uint32_t) override;
        };

        class BitmapPredicate : public IntPredicate {
        private:
            SimpleBitmap bitmap_;
        public:
            BitmapPredicate(uint32_t max);

            void add(uint32_t);

            bool test(uint32_t) override;
        };

        class HashContainer {
        private:
            unordered_map<int32_t, shared_ptr<DataRow>> hashmap_;
            int32_t min_ = INT_MAX;
            int32_t max_ = INT_MIN;

        public:
            HashContainer();

            void add(int32_t key, shared_ptr<DataRow> dataRow);

            shared_ptr<DataRow> get(int32_t key);

            shared_ptr<DataRow> remove(int32_t key);

            uint32_t size();
        };

        class HashBuilder {
        public:
            static unique_ptr<IntPredicate> buildHashPredicate(Table &input, uint32_t);

            static unique_ptr<IntPredicate> buildBitmapPredicate(Table &input, uint32_t);

            static unique_ptr<HashContainer> buildContainer(Table &input, uint32_t, RowBuilder &);

        };

    }

    using namespace join;

    class Join {
    public:
        virtual shared_ptr<Table> join(Table &, Table &) = 0;
    };

    class HashJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        function<bool(DataRow &, DataRow &)> predicate_;
        unique_ptr<RowBuilder> rowBuilder_;
        unique_ptr<HashContainer> container_;
    public:

        HashJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, RowBuilder *builder,
                 function<bool(DataRow &, DataRow &)> pred = TRUE);

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

    protected:
        virtual shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock);
    };


    class HashFilterJoin : public Join {
    private:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        unique_ptr<IntPredicate> predicate_;
    public:
        HashFilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex);

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

    protected:
        shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock);
    };


    class HashExistJoin : public HashJoin {

    public:
        HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                      RowBuilder *rowBuilder,
                      function<bool(DataRow &, DataRow &)> pred = TRUE);

    protected:
        shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock) override;
    };

}
#endif //CHIDATA_LQF_JOIN_H
