//
// Created by harper on 2/25/20.
//

#ifndef CHIDATA_LQF_JOIN_H
#define CHIDATA_LQF_JOIN_H

#include <climits>
#include "data_model.h"

#define JL(x) x
#define JR(x) x | 0x10000
#define JLS(x) x | 0x20000
#define JRS(x) x | 0x30000

namespace lqf {
    using namespace std;

    static function<bool(DataRow &, DataRow &)> TRUE = [](DataRow &a, DataRow &b) { return true; };

    namespace join {

        class JoinBuilder {
        protected:
            bool needkey_;
            bool vertical_;
            uint8_t num_fields_;
            uint8_t num_fields_string_;

            vector<pair<uint8_t, uint8_t>> left_inst_;
            vector<pair<uint8_t, uint8_t>> right_inst_;
            vector<uint32_t> right_col_offsets_;
            vector<uint32_t> right_col_size_;

            void init(initializer_list<int32_t>);

        public:
            JoinBuilder(initializer_list<int32_t>, bool needkey, bool vertical);

            inline bool useVertical() { return vertical_; }

            inline uint8_t numFields() { return num_fields_; }

            inline uint8_t numStringFields() { return num_fields_string_; }

            shared_ptr<MemDataRow> snapshot(DataRow &);
        };

        class RowBuilder : public JoinBuilder {

        public:
            RowBuilder(initializer_list<int32_t>, bool needkey = false);

            void build(DataRow &, DataRow &, DataRow &, int32_t key);
        };

        /// For use with VJoin
        class ColumnBuilder : public JoinBuilder {
        public:
            ColumnBuilder(initializer_list<int32_t>);

            inline const vector<uint32_t> &rightColSize() { return right_col_size_; }

            inline const vector<pair<uint8_t, uint8_t>> &leftInst() { return left_inst_; }

            inline const vector<pair<uint8_t, uint8_t>> &rightInst() { return right_inst_; }
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

            static unique_ptr<HashContainer> buildContainer(Table &input, uint32_t, JoinBuilder *);

        };

    }

    using namespace join;

    class Join {
    public:
        virtual shared_ptr<Table> join(Table &, Table &) = 0;
    };

    class HashBasedJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        unique_ptr<JoinBuilder> builder_;
        unique_ptr<HashContainer> container_;
    public:
        HashBasedJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                      JoinBuilder *builder);

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

    protected:
        virtual void probe(MemTable *, const shared_ptr<Block> &leftBlock) = 0;
    };

    class HashJoin : public HashBasedJoin {
    public:
        HashJoin(uint32_t, uint32_t, RowBuilder *,
                 function<bool(DataRow &, DataRow &)> pred = TRUE);

    protected:
        RowBuilder *rowBuilder_;
        function<bool(DataRow &, DataRow &)> predicate_;

        void probe(MemTable *, const shared_ptr<Block> &) override;
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
        void probe(MemTable *, const shared_ptr<Block> &leftBlock) override;
    };

    /**
     * HashVJoin is used to perform joining on vertical memory table.
     * It allows reading only columns participating joining, and avoids
     * unnecessary data movement between memory tables when multiple
     * joins are performed in sequence.
     */
    class HashColumnJoin : public HashBasedJoin {
    public:
        HashColumnJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, ColumnBuilder *builder);

    protected:
        ColumnBuilder *columnBuilder_;

        void probe(MemTable *, const shared_ptr<Block> &leftBlock) override;
    };

}
#endif //CHIDATA_LQF_JOIN_H
