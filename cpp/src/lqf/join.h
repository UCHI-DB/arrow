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
#define JLR(x) x | 0x40000
#define JRR(x) x | 0x50000

#define COL_HASHER(x) [](DataRow& input) { return input[x].asInt(); }
#define COL_HASHER2(x, y) [](DataRow& input) { return (static_cast<int64_t>(input[x].asInt()) << 32) + input[y].asInt(); }

namespace lqf {
    using namespace std;

    static function<bool(DataRow & , DataRow & )> TRUE = [](DataRow &a, DataRow &b) { return true; };

    namespace join {

        class JoinBuilder {
        protected:
            bool needkey_;
            bool vertical_;

            vector<uint32_t> output_col_offsets_;
            vector<uint32_t> output_col_size_;

            vector<uint32_t> right_col_offsets_;
            vector<uint32_t> right_col_size_;

            vector<pair<uint8_t, uint8_t>> left_inst_;
            vector<pair<uint8_t, uint8_t>> right_inst_;
            vector<pair<uint8_t, uint8_t>> left_raw_;
            vector<pair<uint8_t, uint8_t>> right_raw_;

            void init(initializer_list<int32_t>);

        public:
            JoinBuilder(initializer_list<int32_t>, bool needkey, bool vertical);

            virtual ~JoinBuilder() {}

            inline bool useVertical() { return vertical_; }

            inline vector<uint32_t> &outputColOffset() { return output_col_offsets_; }

            inline vector<uint32_t> &outputColSize() { return output_col_size_; }

            inline vector<uint32_t> &rightColOffset() { return right_col_offsets_; }

            unique_ptr<MemDataRow> snapshot(DataRow &);
        };

        class RowBuilder : public JoinBuilder {

        public:
            RowBuilder(initializer_list<int32_t>, bool needkey = false, bool vertical = false);

            virtual void build(DataRow &, DataRow &, DataRow &, int32_t key);
        };

        /// For use with VJoin
        class ColumnBuilder : public JoinBuilder {
        public:
            ColumnBuilder(initializer_list<int32_t>);

            inline const vector<uint32_t> &rightColSize() { return right_col_size_; }

            inline const vector<pair<uint8_t, uint8_t>> &leftInst() { return left_inst_; }

            inline const vector<pair<uint8_t, uint8_t>> &rightInst() { return right_inst_; }
        };

    }

    namespace hash {
        class IntPredicate {
        public:
            virtual bool test(int64_t) = 0;
        };

        class HashPredicate : public IntPredicate {
        private:
            unordered_set<int64_t> content_;
        public:
            HashPredicate();

            void add(int64_t);

            bool test(int64_t) override;
        };

        class BitmapPredicate : public IntPredicate {
        private:
            SimpleBitmap bitmap_;
        public:
            BitmapPredicate(uint64_t max);

            void add(int64_t);

            bool test(int64_t) override;
        };

        class HashContainer {
        private:
            unordered_map<int64_t, unique_ptr<MemDataRow>> hashmap_;
            int64_t min_ = INT64_MAX;
            int64_t max_ = INT64_MIN;

        public:
            HashContainer();

            void add(int64_t key, unique_ptr<MemDataRow> dataRow);

            MemDataRow *get(int64_t key);

            unique_ptr<MemDataRow> remove(int64_t key);

            inline unordered_map<int64_t, unique_ptr<MemDataRow>> &content() { return hashmap_; }

            uint32_t size();
        };

        class HashMemBlock : public MemBlock {
        private:
            unique_ptr<HashContainer> container_;
            unique_ptr<IntPredicate> predicate_;
        public:
            HashMemBlock(unique_ptr<HashContainer> container);

            HashMemBlock(unique_ptr<IntPredicate> predicate);

            unique_ptr<HashContainer> container();

            unique_ptr<IntPredicate> predicate();
        };

        class HashBuilder {
        public:
            static unique_ptr<IntPredicate> buildHashPredicate(Table &input, uint32_t);

            static unique_ptr<IntPredicate> buildHashPredicate(Table &input, function<int64_t(DataRow & )>);

            static unique_ptr<IntPredicate> buildBitmapPredicate(Table &input, uint32_t);

            static unique_ptr<HashContainer>
            buildContainer(Table &input, uint32_t, function<unique_ptr<MemDataRow>(DataRow & )>);

            static unique_ptr<HashContainer>
            buildContainer(Table &input, function<int64_t(DataRow & )>, function<unique_ptr<MemDataRow>(DataRow & )>);

        };

    }

    using namespace join;
    using namespace hash;

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
        bool outer_ = false;
    public:
        HashBasedJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex,
                      JoinBuilder *builder);

        virtual ~HashBasedJoin() {}

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        inline void useOuter() { outer_ = true; };
    protected:
        virtual void probe(MemTable *, const shared_ptr<Block> &leftBlock) = 0;
    };

    class HashJoin : public HashBasedJoin {
    public:
        HashJoin(uint32_t, uint32_t, RowBuilder *,
                 function<bool(DataRow & , DataRow & )> pred = nullptr);

    protected:
        RowBuilder *rowBuilder_;
        function<bool(DataRow & , DataRow & )> predicate_;

        void probe(MemTable *, const shared_ptr<Block> &) override;
    };

    class HashFilterJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        unique_ptr<IntPredicate> predicate_;
        bool anti_ = false;
    public:
        HashFilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex);

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        void useAnti() { anti_ = true; }

    protected:
        shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock);
    };

    ///
    /// HashExistJoin returns matched records from the hashtable
    ///
    class HashExistJoin : public HashBasedJoin {

    public:
        HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *rowBuilder,
                      function<bool(DataRow & , DataRow & )> pred = nullptr);

    protected:
        function<bool(DataRow & , DataRow & )> predicate_;

        void probe(MemTable *, const shared_ptr<Block> &leftBlock) override;
    };

    class HashNotExistJoin : public HashBasedJoin {
    public:
        HashNotExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *rowBuilder,
                         function<bool(DataRow & , DataRow & )> pred = nullptr);

        shared_ptr<Table> join(Table &, Table &) override;

    protected:
        function<bool(DataRow & , DataRow & )> predicate_;

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
        HashColumnJoin(uint32_t, uint32_t, ColumnBuilder *);

    protected:
        ColumnBuilder *columnBuilder_;

        void probe(MemTable *, const shared_ptr<Block> &leftBlock) override;
    };

    namespace powerjoin {

        class PowerHashBasedJoin : public Join {
        protected:
            function<int64_t(DataRow & )> left_key_maker_;
            function<int64_t(DataRow & )> right_key_maker_;
            unique_ptr<JoinBuilder> builder_;
            unique_ptr<HashContainer> container_;
            function<bool(DataRow & , DataRow & )> predicate_;
            bool outer_ = false;
        public:
            PowerHashBasedJoin(function<int64_t(DataRow & )>, function<int64_t(DataRow & )>,
                               JoinBuilder *, function<bool(DataRow & , DataRow & )> pred = nullptr);

            virtual shared_ptr<Table> join(Table &left, Table &right) override;

            inline void useOuter() { outer_ = true; }

        protected:
            virtual void probe(MemTable *, const shared_ptr<Block> &) = 0;
        };

        class PowerHashJoin : public PowerHashBasedJoin {
        public:
            PowerHashJoin(function<int64_t(DataRow & )>, function<int64_t(DataRow & )>, RowBuilder *,
                          function<bool(DataRow & , DataRow & )> pred);

        protected:
            RowBuilder *rowBuilder_;

            void probe(MemTable *, const shared_ptr<Block> &) override;
        };

        class PowerHashColumnJoin : public PowerHashBasedJoin {
        public:
            PowerHashColumnJoin(function<int64_t(DataRow & )>, function<int64_t(DataRow & )>, ColumnBuilder *);

        protected:
            ColumnBuilder *columnBuilder_;

            void probe(MemTable *, const shared_ptr<Block> &leftBlock) override;
        };

        class PowerHashFilterJoin : public Join {
        protected:
            function<int64_t(DataRow & )> left_key_maker_;
            function<int64_t(DataRow & )> right_key_maker_;
            unique_ptr<IntPredicate> predicate_;
            bool anti_ = false;
        public:
            PowerHashFilterJoin(function<int64_t(DataRow & )>, function<int64_t(DataRow & )>);

            virtual shared_ptr<Table> join(Table &left, Table &right) override;

            void useAnti() { anti_ = true; }

        protected:
            shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock);
        };
    }

    namespace blockjoin {

    }
}
#endif //CHIDATA_LQF_JOIN_H
