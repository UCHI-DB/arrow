//
// Created by Harper on 6/17/20.
//

#ifndef ARROW_AGG_H
#define ARROW_AGG_H

#include <unordered_set>
#include "data_model.h"
#include "data_container.h"
#include "rowcopy.h"
#include "parallel.h"

namespace lqf {
    using namespace datacontainer;
    using namespace rowcopy;
    namespace agg {

        struct AsDouble {
            static double get(DataField &df) { return df.asDouble(); }

            static double MAX;

            static double MIN;
        };

        struct AsInt {
            static int32_t get(DataField &df) { return df.asInt(); }

            static int MAX;

            static int MIN;
        };

        class AggField {
        protected:
            uint32_t size_;
            uint32_t read_idx_;
            uint32_t write_idx_;
            DataField value_;
            bool need_dump_;
        public:
            AggField(uint32_t size, uint32_t read_idx, bool need_dump = false);

            virtual ~AggField() = default;

            virtual void attach(DataRow &);

            virtual void init();

            virtual void reduce(DataRow &) = 0;

            virtual void merge(AggField &) = 0;

            inline bool need_dump() { return need_dump_; }

            virtual void dump();

            inline void write_at(uint32_t at) { write_idx_ = at; }

            inline uint32_t size() { return size_; }
        };

        class Count : public AggField {
        public:
            Count();

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        class IntDistinctCount : public AggField {
        protected:
            unordered_set<int32_t> *distinct_;
        public:
            IntDistinctCount(uint32_t);

            void attach(DataRow &) override;

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;

            void dump() override;
        };

        class IntSum : public AggField {
        public:
            IntSum(uint32_t);

            virtual void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        class DoubleSum : public AggField {
        public:
            DoubleSum(uint32_t);

            virtual void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        template<typename ACC>
        class Avg : public AggField {
        protected:
            DataField count_;
        public:
            Avg(uint32_t);

            void attach(DataRow &) override;

            void init() override;

            virtual void reduce(DataRow &) override;

            void merge(AggField &) override;

            void dump() override;
        };

        template
        class Avg<AsInt>;

        using IntAvg = Avg<AsInt>;

        template
        class Avg<AsDouble>;

        using DoubleAvg = Avg<AsDouble>;

        template<typename ACC>
        class Max : public AggField {
        public:
            Max(uint32_t);

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        template
        class Max<AsInt>;

        using IntMax = Max<AsInt>;

        template
        class Max<AsDouble>;

        using DoubleMax = Max<AsDouble>;

        template<typename ACC>
        class Min : public AggField {
        public:
            Min(uint32_t);

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        template
        class Min<AsInt>;

        using IntMin = Min<AsInt>;

        template
        class Min<AsDouble>;

        using DoubleMin = Min<AsDouble>;

        class AggReducer {
        protected:
            vector<unique_ptr<AggField>> fields_;
            DataRow *storage_;
            Snapshoter *header_copier_;
            function<void(DataRow &, DataRow &)> *row_copier_;
        public:
            AggReducer(Snapshoter *, function<void(DataRow &, DataRow &)> *, vector<AggField *>,
                       const vector<uint32_t> &);

            AggReducer(Snapshoter *, function<void(DataRow &, DataRow &)> *, AggField *,
                       uint32_t);

            void attach(DataRow &);

            void init(DataRow &);

            void reduce(DataRow &);

            void dump();

            void merge(AggReducer &);

            void assign(AggReducer &);

            inline vector<unique_ptr<AggField>> &fields() { return fields_; }

            inline DataRow *storage() { return storage_; }

            inline function<void(DataRow &, DataRow &)> *row_copier() {
                return row_copier_;
            }
        };

        class HashCore {
        protected:
            unique_ptr<AggReducer> reducer_;
            function<uint64_t(DataRow &)> &hasher_;
            MemRowMap map_;
            bool need_dump_;
        public:
            HashCore(const vector<uint32_t> &, unique_ptr<AggReducer>, function<uint64_t(DataRow &)> &, bool);

            void reduce(DataRow &row);

            void merge(HashCore &another);

            void dump(MemTable &table, function<bool(DataRow &)>);

        };

        class HashSmallCore {
        protected:
            function<unique_ptr<AggReducer>()> reducer_gen_;
            function<uint64_t(DataRow &)> &hasher_;
            const vector<uint32_t> &col_offset_;
            unordered_map<uint64_t, unique_ptr<AggReducer>> map_;
            MemRowVector rows_;
            bool need_dump_;
        public:
            HashSmallCore(const vector<uint32_t> &, function<unique_ptr<AggReducer>()>, function<uint64_t(DataRow &)> &,
                          bool);

            void reduce(DataRow &row);

            void merge(HashSmallCore &another);

            void dump(MemTable &table, function<bool(DataRow &)>);

        };

        class TableCore {
        protected:
            function<unique_ptr<AggReducer>()> reducer_gen_;
            function<uint32_t(DataRow &)> &indexer_;
            const vector<uint32_t> &col_offset_;
            vector<unique_ptr<AggReducer>> table_;
            MemRowVector rows_;
            bool need_dump_;
        public:
            TableCore(uint32_t, const vector<uint32_t> &, function<unique_ptr<AggReducer>()>,
                      function<uint32_t(DataRow &)> &, bool);

            void reduce(DataRow &row);

            void merge(TableCore &another);

            void dump(MemTable &table, function<bool(DataRow &)>);

        };

        class SimpleCore {
        protected:
            unique_ptr<AggReducer> reducer_;
            MemDataRow storage_;
            bool need_dump_;
        public:
            SimpleCore(const vector<uint32_t> &, unique_ptr<AggReducer>, bool);

            void reduce(DataRow &row);

            void merge(SimpleCore &another);

            void dump(MemTable &table, function<bool(DataRow &)>);

        };

        namespace recording {

            class RecordingAggField : public AggField {
            protected:
                uint32_t key_idx_;
                unordered_set<int32_t> *keys_;
            public:
                RecordingAggField(uint32_t read_idx, uint32_t key_idx);

                void attach(DataRow &) override;

                virtual void init() override;

                virtual void merge(AggField &) override;

                inline unordered_set<int32_t> *keys() { return keys_; }
            };

            template<typename ACC>
            class RecordingMin : public RecordingAggField {
            public:
                RecordingMin(uint32_t, uint32_t);

                virtual void init() override;

                void reduce(DataRow &) override;

                void merge(AggField &) override;
            };

            template
            class RecordingMin<AsInt>;

            using RecordingIntMin = RecordingMin<AsInt>;

            template
            class RecordingMin<AsDouble>;

            using RecordingDoubleMin = RecordingMin<AsDouble>;

            template<typename ACC>
            class RecordingMax : public RecordingAggField {
            public:
                RecordingMax(uint32_t, uint32_t);

                virtual void init() override;

                void reduce(DataRow &) override;

                void merge(AggField &) override;
            };

            template
            class RecordingMax<AsInt>;

            using RecordingIntMax = RecordingMax<AsInt>;

            template
            class RecordingMax<AsDouble>;

            using RecordingDoubleMax = RecordingMax<AsDouble>;

//            class RecordingAggReducer : public AggReducer {
//            protected:
//                RecordingAggField *field_;
//                vector<shared_ptr<vector<int32_t>>> keys_;
//            public:
//                RecordingAggReducer(Snapshoter *, function<void(DataRow &, DataRow &)> *,
//                                    RecordingAggField *, uint32_t);
//
//                void init(DataRow &);
//
//                void assign(RecordingAggReducer &);
//
//                inline RecordingAggField *field() { return field_; }
//            };

            class RecordingHashCore : public HashCore {
            protected:
                uint32_t write_key_index_;
            public:
                RecordingHashCore(const vector<uint32_t> &, unique_ptr<AggReducer>,
                                  function<uint64_t(DataRow &)> &);

                void dump(MemTable &table, function<bool(DataRow &)>);
            };

            class RecordingSimpleCore : public SimpleCore {
            protected:
                uint32_t write_key_index_;
            public:
                RecordingSimpleCore(const vector<uint32_t> &, unique_ptr<AggReducer>);

                void dump(MemTable &table, function<bool(DataRow &)>);
            };
        }
    }

    using namespace parallel;
    using namespace agg;
    using namespace agg::recording;

    template<typename CORE>
    class Agg : public Node {
    protected:
        vector<uint32_t> col_offset_;
        vector<uint32_t> col_size_;
        bool need_field_dump_;

        unique_ptr<Snapshoter> header_copier_;
        unique_ptr<function<void(DataRow &, DataRow &)>> row_copier_;

        function<vector<AggField *>()> fields_gen_;
        function<RecordingAggField *()> field_gen_;
        vector<uint32_t> fields_start_;

        function<bool(DataRow &)> predicate_;
        bool vertical_;

        virtual shared_ptr<CORE> processBlock(const shared_ptr<Block> &block);

        virtual shared_ptr<CORE> makeCore();

        virtual unique_ptr<agg::AggReducer> createReducer();

        virtual unique_ptr<agg::AggReducer> createRecordingReducer();

    public:
        Agg(unique_ptr<Snapshoter>, function<vector<AggField *>()>, function<bool(DataRow &)> pred = nullptr,
            bool vertical = false);

        Agg(unique_ptr<Snapshoter>, function<RecordingAggField *()>, function<bool(DataRow &)> pred = nullptr,
            bool vertical = false);

        virtual ~Agg() = default;

        virtual unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> agg(Table &input);

        inline void setPredicate(function<bool(DataRow &)> p) { predicate_ = p; }
    };

    class HashAgg : public Agg<agg::HashCore> {
    protected:
        function<uint64_t(DataRow &)> hasher_;

        shared_ptr<agg::HashCore> makeCore() override;

    public:
        HashAgg(function<uint64_t(DataRow &)>, unique_ptr<Snapshoter>,
                function<vector<agg::AggField *>()>,
                function<bool(DataRow &)> pred = nullptr, bool vertical = false);
    };

    class HashSmallAgg : public Agg<agg::HashSmallCore> {
    protected:
        function<uint64_t(DataRow &)> hasher_;

        shared_ptr<agg::HashSmallCore> makeCore() override;

    public:
        HashSmallAgg(function<uint64_t(DataRow &)>, unique_ptr<Snapshoter>,
                     function<vector<agg::AggField *>()>,
                     function<bool(DataRow &)> pred = nullptr, bool vertical = false);
    };

    class TableAgg : public Agg<agg::TableCore> {
    protected:
        uint32_t table_size_;
        function<uint32_t(DataRow &)> indexer_;

        shared_ptr<agg::TableCore> makeCore() override;

    public:
        TableAgg(uint32_t, function<uint32_t(DataRow &)>, unique_ptr<Snapshoter>,
                 function<vector<agg::AggField *>()>, function<bool(DataRow &)>
                 pred = nullptr, bool vertical = false);
    };

    class SimpleAgg : public Agg<agg::SimpleCore> {
    protected:
        shared_ptr<agg::SimpleCore> makeCore() override;

    public:
        SimpleAgg(function<vector<agg::AggField *>()>,
                  function<bool(DataRow &)> pred = nullptr, bool vertical = false);

    };

    using namespace agg;
    using namespace agg::recording;

    class RecordingHashAgg : public Agg<RecordingHashCore> {
    protected:
        function<uint64_t(DataRow &)> hasher_;

        shared_ptr<RecordingHashCore> makeCore() override;

    public:
        RecordingHashAgg(function<uint64_t(DataRow &)>, unique_ptr<Snapshoter>,
                         function<RecordingAggField *()>,
                         function<bool(DataRow &)> pred = nullptr, bool vertical = false);

    };

    class RecordingSimpleAgg : public Agg<RecordingSimpleCore> {
    protected:

        shared_ptr<RecordingSimpleCore> makeCore() override;

    public:
        RecordingSimpleAgg(function<RecordingAggField *()>,
                           function<bool(DataRow &)> pred = nullptr, bool vertical = false);
    };

    class StripeHashAgg : public Node {
    protected:
        uint32_t num_stripe_;
        function<uint64_t(DataRow &)> hasher_;
        function<uint64_t(DataRow &)> stripe_hasher_;
        unique_ptr<Snapshoter> stripe_copier_;
        unique_ptr<Snapshoter> header_copier_;
        unique_ptr<function<void(DataRow &, DataRow &)>> data_copier_;
        function<vector<AggField *>()> fields_gen_;

        vector<uint32_t> col_offset_;
        vector<uint32_t> col_size_;
        vector<uint32_t> fields_start_;
        bool need_field_dump_;

        shared_ptr<vector<shared_ptr<MemRowVector>>> makeStripes(const shared_ptr<Block> &);

        shared_ptr<vector<shared_ptr<HashCore>>> aggStripes(const shared_ptr<vector<shared_ptr<MemRowVector>>> &);

        shared_ptr<vector<shared_ptr<HashCore>>>
        mergeCore(shared_ptr<vector<shared_ptr<HashCore>>>, shared_ptr<vector<shared_ptr<HashCore>>>);

        unique_ptr<AggReducer> createReducer();

        shared_ptr<HashCore> makeCore();

    public:
        StripeHashAgg(uint32_t num_stripe, function<uint64_t(DataRow &)>, function<uint64_t(DataRow &)>,
                      unique_ptr<Snapshoter>, unique_ptr<Snapshoter>, function<vector<agg::AggField *>()>);

        virtual ~StripeHashAgg() = default;

        virtual unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> agg(Table &input);
    };
}


#endif //ARROW_AGG_H
