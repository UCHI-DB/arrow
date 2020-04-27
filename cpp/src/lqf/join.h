//
// Created by harper on 2/25/20.
//

#ifndef CHIDATA_LQF_JOIN_H
#define CHIDATA_LQF_JOIN_H

#include <climits>
#include "data_model.h"
#include "concurrent.h"

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
    using namespace lqf::phasecon;

    static function<bool(DataRow &, DataRow &)> TRUE = [](DataRow &a, DataRow &b) { return true; };

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
            vector<pair<uint8_t, uint8_t>> left_raw_;

            vector<pair<uint8_t, uint8_t>> right_read_inst_;
            vector<pair<uint8_t, uint8_t>> right_read_raw_;

            vector<pair<uint8_t, uint8_t>> right_write_inst_;

            void init(initializer_list<int32_t>);

        public:
            JoinBuilder(initializer_list<int32_t>, bool needkey, bool vertical);

            virtual ~JoinBuilder() {}

            inline bool useVertical() { return vertical_; }

            inline vector<uint32_t> &outputColOffset() { return output_col_offsets_; }

            inline vector<uint32_t> &outputColSize() { return output_col_size_; }

            inline vector<uint32_t> &rightColOffset() { return right_col_offsets_; }

            virtual unique_ptr<MemDataRow> snapshot(DataRow &);
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

            inline const vector<pair<uint8_t, uint8_t>> &rightInst() { return right_write_inst_; }
        };

    }

    namespace hashjoin {

        template<typename DTYPE>
        class IntPredicate {
            using ktype = typename DTYPE::type;
        public:
            virtual bool test(ktype) = 0;
        };

        using Int32Predicate = IntPredicate<Int32>;
        using Int64Predicate = IntPredicate<Int64>;

        template<typename DTYPE>
        class HashPredicate : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        private:
            PhaseConcurrentHashSet<DTYPE> content_;
            atomic<ktype> min_;
            atomic<ktype> max_;
        public:
            HashPredicate();

            void add(ktype);

            bool test(ktype) override;
        };

        using Hash32Predicate = HashPredicate<Int32>;
        using Hash64Predicate = HashPredicate<Int64>;

        class BitmapPredicate : public Int32Predicate {
        private:
            SimpleBitmap bitmap_;
        public:
            BitmapPredicate(uint32_t max);

            void add(int32_t);

            bool test(int32_t) override;
        };

        template<typename DTYPE>
        class HashContainer : public IntPredicate<DTYPE> {
            using ktype = typename DTYPE::type;
        protected:
            PhaseConcurrentHashMap<DTYPE, MemDataRow *> hashmap_;
            atomic<ktype> min_;
            atomic<ktype> max_;

        public:
            HashContainer();

            void add(ktype key, unique_ptr<MemDataRow> dataRow);

            bool test(ktype) override;

            MemDataRow *get(ktype key);

            unique_ptr<MemDataRow> remove(ktype key);

            unique_ptr<lqf::Iterator<std::pair<ktype, MemDataRow *>>> iterator();

            inline uint32_t size() { return hashmap_.size(); }

            inline ktype min() { return min_.load(); }

            inline ktype max() { return max_.load(); }
        };

        using Hash32Container = HashContainer<Int32>;
        using Hash64Container = HashContainer<Int64>;

        template<typename CONTENT>
        class HashMemBlock : public MemBlock {
        private:
            shared_ptr<CONTENT> content_;
        public:
            HashMemBlock(shared_ptr<CONTENT> predicate);

            shared_ptr<CONTENT> content();
        };

        class HashBuilder {
        public:
            static shared_ptr<Int32Predicate> buildHashPredicate(Table &input, uint32_t);

            static shared_ptr<Int64Predicate> buildHashPredicate(Table &input, function<int64_t(DataRow &)>);

            static shared_ptr<Int32Predicate> buildBitmapPredicate(Table &input, uint32_t);

            static shared_ptr<Hash32Container>
            buildContainer(Table &input, uint32_t, function<unique_ptr<MemDataRow>(DataRow &)>);

            static shared_ptr<Hash64Container>
            buildContainer(Table &input, function<int64_t(DataRow &)>, function<unique_ptr<MemDataRow>(DataRow &)>);
        };
    }

    using namespace join;
    using namespace hashjoin;

    class Join {
    public:
        virtual shared_ptr<Table> join(Table &, Table &) = 0;
    };

    class HashBasedJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        unique_ptr<JoinBuilder> builder_;
        shared_ptr<Hash32Container> container_;
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
                 function<bool(DataRow &, DataRow &)> pred = nullptr);

    protected:
        RowBuilder *rowBuilder_;
        function<bool(DataRow &, DataRow &)> predicate_;

        void probe(MemTable *, const shared_ptr<Block> &) override;
    };

    class HashFilterJoin : public Join {
    protected:
        uint32_t leftKeyIndex_;
        uint32_t rightKeyIndex_;
        shared_ptr<Int32Predicate> predicate_;
        bool anti_ = false;
    public:
        HashFilterJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex);

        virtual shared_ptr<Table> join(Table &left, Table &right) override;

        void useAnti() { anti_ = true; }

    protected:
        shared_ptr<Block> probe(const shared_ptr<Block> &leftBlock);
    };

    /*
     * HashExistJoin works by creating a hash table containing all records to be checked,
     * and probe using another table. All records in the hashtable that has at least one match
     * will be output. It is different from HashFilterJoin in the case that the results are
     * output from the build table, not the probe table.
     *
     * When there's no predicate present, we remove a record from the hash table when one
     * match appears. This makes sure a record only appears in the output once.
     *
     * When there's a predicate, we fetch a record from the hash table, apply the predicate.
     * If the result is success, we mark the key in a bitmap. Keys already in the bitmap will
     * not be further checked. Bitmaps from different blocks will be logical or to obtain the final
     * result. The reason for this complicated operation is that with existence of a predicate,
     * we need to perform either a remove-check-putback or get-check-remove operation.
     * These operations are not parallelizable.
     */


    class HashExistJoin : public HashBasedJoin {

    public:
        HashExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *rowBuilder,
                      function<bool(DataRow &, DataRow &)> pred = nullptr);

        shared_ptr<Table> join(Table &, Table &) override;

    protected:
        function<bool(DataRow &, DataRow &)> predicate_;

        void probe(MemTable *, const shared_ptr<Block> &leftBlock) override;

        shared_ptr<SimpleBitmap> probeWithPredicate(const shared_ptr<Block> &leftBlock);
    };

    class HashNotExistJoin : public HashExistJoin {
    public:
        HashNotExistJoin(uint32_t leftKeyIndex, uint32_t rightKeyIndex, JoinBuilder *rowBuilder,
                         function<bool(DataRow &, DataRow &)> pred = nullptr);

        shared_ptr<Table> join(Table &, Table &) override;

    protected:

        void probe(MemTable *, const shared_ptr<Block> &) override;

    };

    /**
     * HashColumnJoin is used to perform joining on vertical memory table.
     * It allows reading only columns participating joining, and avoids
     * unnecessary data movement between memory tables when multiple
     * joins are performed in sequence.
     */
    class HashColumnJoin : public HashBasedJoin {
    public:
        HashColumnJoin(uint32_t, uint32_t, ColumnBuilder *);

    protected:
        ColumnBuilder *columnBuilder_;

        void probe(MemTable *, const shared_ptr<Block> &) override;
    };

    namespace powerjoin {

        class PowerHashBasedJoin : public Join {
        protected:
            function<int64_t(DataRow &)> left_key_maker_;
            function<int64_t(DataRow &)> right_key_maker_;
            unique_ptr<JoinBuilder> builder_;
            shared_ptr<Hash64Container> container_;
            function<bool(DataRow &, DataRow &)> predicate_;
            bool outer_ = false;
        public:
            PowerHashBasedJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>,
                               JoinBuilder *, function<bool(DataRow &, DataRow &)> pred = nullptr);

            virtual shared_ptr<Table> join(Table &left, Table &right) override;

            inline void useOuter() { outer_ = true; }

        protected:
            virtual void probe(MemTable *, const shared_ptr<Block> &) = 0;
        };

        class PowerHashJoin : public PowerHashBasedJoin {
        public:
            PowerHashJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>, RowBuilder *,
                          function<bool(DataRow &, DataRow &)> pred = nullptr);

        protected:
            RowBuilder *rowBuilder_;

            void probe(MemTable *, const shared_ptr<Block> &) override;
        };

        class PowerHashColumnJoin : public PowerHashBasedJoin {
        public:
            PowerHashColumnJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>, ColumnBuilder *);

        protected:
            ColumnBuilder *columnBuilder_;

            void probe(MemTable *, const shared_ptr<Block> &) override;
        };

        class PowerHashFilterJoin : public Join {
        protected:
            function<int64_t(DataRow &)> left_key_maker_;
            function<int64_t(DataRow &)> right_key_maker_;
            shared_ptr<Int64Predicate> predicate_;
            bool anti_ = false;
        public:
            PowerHashFilterJoin(function<int64_t(DataRow &)>, function<int64_t(DataRow &)>);

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
