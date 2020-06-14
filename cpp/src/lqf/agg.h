//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_AGG_H
#define LQF_OPERATOR_AGG_H

#include "data_model.h"
#include "parallel.h"

#define AGI(x) x
#define AGD(x) 0x10000 | x
#define AGB(x) 0x20000 | x
#define AGR(x) 0x30000 | x

using namespace std;
using namespace std::placeholders;
namespace lqf {

    namespace agg {
        class AggField {
        protected:
            uint32_t readIndex_;
        public:
            DataField storage_;

            AggField(uint32_t readIndex);

            virtual ~AggField() = default;

            virtual void init() = 0;

            virtual void reduce(DataRow &) = 0;

            virtual void merge(AggField &) = 0;

            virtual void dump() {}
        };

        class AggRecordingField : public AggField {
        protected:
            vector<int32_t> keys_;
            uint32_t keyIndex_;
        public:
            AggRecordingField(uint32_t rIndex, uint32_t kIndex);

            virtual ~AggRecordingField() = default;

            vector<int32_t> &keys();
        };


        struct AsDouble {
            static double get(DataField &df) { return df.asDouble(); }
        };

        struct AsInt {
            static int32_t get(DataField &df) { return df.asInt(); }
        };

        template<typename T, typename ACC>
        class Sum : public AggField {
        protected:
            T *value_;
        public:
            Sum(uint32_t index);

            void init() override;

            virtual void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        using IntSum = Sum<int32_t, AsInt>;
        using DoubleSum = Sum<double, AsDouble>;

        class Count : public AggField {
        protected:
            int32_t *count_;
        public:
            Count();

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        template<typename T, typename ACC>
        class Max : public AggField {
        protected:
            T *value_;
        public:
            Max(uint32_t rIndex);

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        using IntMax = Max<int32_t, AsInt>;
        using DoubleMax = Max<double, AsDouble>;

        template<typename T, typename ACC>
        class RecordingMax : public AggRecordingField {
        protected:
            T *value_;
        public:
            RecordingMax(uint32_t vIndex, uint32_t kIndex);

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        using IntRecordingMax = RecordingMax<int32_t, AsInt>;
        using DoubleRecordingMax = RecordingMax<double, AsDouble>;

        template<typename T, typename ACC>
        class Min : public AggField {
        protected:
            T *value_;
        public:
            Min(uint32_t rIndex);

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        using IntMin = Min<int32_t, AsInt>;
        using DoubleMin = Min<double, AsDouble>;

        template<typename T, typename ACC>
        class RecordingMin : public AggRecordingField {
        protected:
            T *value_;
        public:
            RecordingMin(uint32_t vIndex, uint32_t kIndex);

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;
        };

        using IntRecordingMin = RecordingMin<int32_t, AsInt>;
        using DoubleRecordingMin = RecordingMin<double, AsDouble>;

        template<typename T, typename ACC>
        class Avg : public AggField {
        protected:
            T value_;
            uint32_t count_;
        public:
            Avg(uint32_t rIndex);

            virtual ~Avg() = default;

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;

            void dump() override;
        };

        using IntAvg = Avg<int32_t, AsInt>;
        using DoubleAvg = Avg<double, AsDouble>;

        template<typename T, typename ACC>
        class DistinctCount : public AggField {
        protected:
            unordered_set<T> values_;
        public:
            DistinctCount(uint32_t rIndex);

            virtual ~DistinctCount() = default;

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;

            void dump() override;
        };

        using IntDistinctCount = DistinctCount<int32_t, AsInt>;

        class AggReducer {
        protected:
            uint32_t header_size_;
            MemDataRow storage_;
            vector<unique_ptr<AggField>> fields_;
        public:
            AggReducer(uint32_t numHeaders, vector<AggField *> fields);

            AggReducer(const vector<uint32_t> &, vector<AggField *> fields);

            virtual ~AggReducer() = default;

            inline uint32_t header_size() { return header_size_; }

            inline MemDataRow &storage() { return storage_; }

            inline vector<unique_ptr<AggField>> &fields() { return fields_; }

            void reduce(DataRow &);

            void merge(AggReducer &reducer);

            virtual void dump(MemDataRow &);

            virtual uint32_t size() { return 1; }
        };

        class AggRecordingReducer : public AggReducer {
        protected:
            AggRecordingField *field_;
        public:

            AggRecordingReducer(vector<uint32_t> &, AggRecordingField *field);

            virtual ~AggRecordingReducer() = default;

            void dump(MemDataRow &) override;

            uint32_t size() override;
        };

    }
    using namespace agg;
    using namespace parallel;

    template<typename CORE>
    class Agg : public Node {
    protected:
        vector<uint32_t> output_col_size_;
        bool vertical_;
        function<bool(DataRow &)> predicate_;

        virtual shared_ptr<CORE> processBlock(const shared_ptr<Block> &block);

        virtual shared_ptr<CORE> makeCore();

    public:
        Agg(const vector<uint32_t> &output_col_size, bool vertical = false);

        virtual ~Agg() = default;

        virtual unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> agg(Table &input);

        void useVertical();

        void setPredicate(function<bool(DataRow &)>);
    };

    class CoreMaker {
    protected:
        bool useRecording_ = false;

        vector<uint32_t> output_offset_;
        vector<uint32_t> output_col_size_;

        vector<pair<uint32_t, uint32_t>> int_fields_;
        vector<pair<uint32_t, uint32_t>> double_fields_;
        vector<pair<uint32_t, uint32_t>> bytearray_fields_;
        vector<pair<uint32_t, uint32_t>> raw_fields_;

        function<vector<AggField *>()> agg_fields_;

        function<unique_ptr<AggReducer>(DataRow &)> headerInit();

    public:
        CoreMaker(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>);

        virtual ~CoreMaker() = default;

        unique_ptr<AggReducer> initHeader(DataRow &row);

        unique_ptr<AggReducer> initRecordingHeader(DataRow &row);

        void useRecording();
    };

    class HashCore {
    protected:
        unordered_map<uint64_t, unique_ptr<AggReducer>> container_;
        function<uint64_t(DataRow &)> hasher_;
        function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
    public:
        HashCore(function<uint64_t(DataRow &)> hasher,
                 function<unique_ptr<AggReducer>(DataRow &)> headerInit);

        virtual ~HashCore() = default;

        void consume(DataRow &row);

        inline uint32_t size() { return container_.size(); };

        void reduce(HashCore &another);

        void dump(MemTable &table, function<bool(DataRow &)>);
    };

    class HashAgg : public Agg<HashCore>, public CoreMaker {
        function<uint64_t(DataRow &)> hasher_;
    public:
        HashAgg(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>,
                function<uint64_t(DataRow &)>, bool vertical = false);

        virtual ~HashAgg() = default;

    protected:
        shared_ptr<HashCore> makeCore() override;
    };

    class TableCore {
    private:
        vector<unique_ptr<AggReducer>> container_;
        function<uint32_t(DataRow &)> indexer_;
        function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
    public:
        TableCore(uint32_t tableSize, function<uint32_t(DataRow &)> indexer,
                  function<unique_ptr<AggReducer>(DataRow &)> headerInit);

        virtual ~TableCore() = default;

        void consume(DataRow &row);

        inline uint32_t size() { return container_.size(); }

        void reduce(TableCore &another);

        void dump(MemTable &table, function<bool(DataRow &)>);
    };

    class TableAgg : public Agg<TableCore>, public CoreMaker {
        uint32_t table_size_;
        function<uint32_t(DataRow &)> indexer_;
    public:
        TableAgg(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>,
                 uint32_t, function<uint32_t(DataRow &)>);

        virtual ~TableAgg() = default;

    protected:
        shared_ptr<TableCore> makeCore() override;
    };

    class SimpleCore {
    private:
        function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
        unique_ptr<AggReducer> reducer_;
    public:
        SimpleCore(function<unique_ptr<AggReducer>(DataRow &)> headerInit);

        virtual ~SimpleCore() = default;

        void consume(DataRow &row);

        inline uint32_t size() { return 1; }

        void reduce(SimpleCore &another);

        void dump(MemTable &table, function<bool(DataRow &)>);
    };

    class SimpleAgg : public Agg<SimpleCore>, public CoreMaker {
    public:
        SimpleAgg(const vector<uint32_t> &, function<vector<AggField *>()>);

        virtual ~SimpleAgg() = default;

    protected:
        shared_ptr<SimpleCore> makeCore() override;
    };

    class StripeAgg : public Node {
    protected:
        uint32_t num_stripes_;

        void processBlock(shared_ptr<Block> &);

        void processStripe(vector<uint32_t>*);
    public:
        StripeAgg(uint32_t);

        virtual ~StripeAgg() = default;

        shared_ptr<Table> agg(Table &input);

        virtual unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;
    };
}
#endif //LQF_OPERATOR_AGG_H
