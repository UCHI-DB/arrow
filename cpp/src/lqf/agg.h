//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_AGG_H
#define LQF_OPERATOR_AGG_H

#include "data_model.h"

#define AGI(x) x
#define AGD(x) 0x10000 | x
#define AGB(x) 0x20000 | x
#define AGR(x) 0x30000 | x

using namespace std;
using namespace std::placeholders;
namespace lqf {

    class AggField {
    protected:
        uint32_t readIndex_;
    public:
        DataField storage_;

        AggField(uint32_t readIndex);

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

        vector<int32_t> &keys();
    };

    namespace agg {

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

            void init() override;

            void reduce(DataRow &) override;

            void merge(AggField &) override;

            void dump() override;
        };

        using IntDistinctCount= DistinctCount<int32_t, AsInt>;
    }

    class AggReducer {
    protected:
        uint32_t header_size_;
        MemDataRow storage_;
        vector<unique_ptr<AggField>> fields_;
    public:
        AggReducer(uint32_t numHeaders, vector<AggField *> fields);

        AggReducer(const vector<uint32_t> &, vector<AggField *> fields);

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

        void dump(MemDataRow &) override;

        uint32_t size() override;
    };

    template<typename CORE>
    class Agg {
    protected:
        vector<uint32_t> output_col_size_;
        bool vertical_;
        function<bool(DataRow & )> predicate_;

        virtual unique_ptr<CORE> processBlock(const shared_ptr<Block> &block);

        virtual unique_ptr<CORE> makeCore();

    public:
        Agg(const vector<uint32_t> &output_col_size, bool vertical = false);

        shared_ptr<Table> agg(Table &input);

        void useVertical();

        void setPredicate(function<bool(DataRow & )>);
    };

    template<typename CORE>
    class DictAgg : public Agg<CORE> {
    protected:
        vector<pair<uint32_t, uint32_t>> need_trans_;

        unique_ptr<CORE> processBlock(const shared_ptr<Block> &block) override;

    public:
        DictAgg(const vector<uint32_t> &, initializer_list<pair<uint32_t, uint32_t>>);
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

        function<unique_ptr<AggReducer>(DataRow & )> headerInit();

    public:
        CoreMaker(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>);

        unique_ptr<AggReducer> initHeader(DataRow &row);

        unique_ptr<AggReducer> initRecordingHeader(DataRow &row);

        void useRecording();
    };

    class HashCore {
    protected:
        unordered_map<uint64_t, unique_ptr<AggReducer>> container_;
        function<uint64_t(DataRow & )> hasher_;
        function<unique_ptr<AggReducer>(DataRow & )> headerInit_;
    public:
        HashCore(function<uint64_t(DataRow & )> hasher,
                 function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        virtual ~HashCore();

        void consume(DataRow &row);

        void reduce(HashCore &another);

        void dump(MemTable &table, function<bool(DataRow & )>);
    };

    class HashAgg : public Agg<HashCore>, public CoreMaker {
        function<uint64_t(DataRow & )> hasher_;
    public:
        HashAgg(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>,
                function<uint64_t(DataRow & )>, bool vertical = false);

    protected:
        unique_ptr<HashCore> makeCore() override;
    };

    class HashDictCore : public HashCore {
    protected:
        unordered_map<string, unique_ptr<AggReducer>> translated_;
    public:
        HashDictCore(function<uint64_t(DataRow & )> hasher,
                     function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        void reduce(HashDictCore &another);

        void dump(MemTable &table, function<bool(DataRow & )>);

        void translate(vector<pair<uint32_t, uint32_t>> &, function<void(DataField & , uint32_t, uint32_t)>);
    };

    class HashDictAgg : public DictAgg<HashDictCore>, public CoreMaker {
        function<uint64_t(DataRow & )> hasher_;
    public:
        HashDictAgg(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>,
                    function<uint64_t(DataRow & )>, initializer_list<pair<uint32_t, uint32_t>>);

    protected:
        unique_ptr<HashDictCore> makeCore() override;
    };

    class TableCore {
    private:
        vector<unique_ptr<AggReducer>> container_;
        function<uint32_t(DataRow & )> indexer_;
        function<unique_ptr<AggReducer>(DataRow & )> headerInit_;
    public:
        TableCore(uint32_t tableSize, function<uint32_t(DataRow & )> indexer,
                  function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        virtual ~TableCore();

        void consume(DataRow &row);

        void reduce(TableCore &another);

        void dump(MemTable &table, function<bool(DataRow & )>);
    };

    class TableAgg : public Agg<TableCore>, public CoreMaker {
        uint32_t table_size_;
        function<uint32_t(DataRow & )> indexer_;
    public:
        TableAgg(const vector<uint32_t> &, const initializer_list<int32_t> &, function<vector<AggField *>()>,
                 uint32_t, function<uint32_t(DataRow & )>);

    protected:
        unique_ptr<TableCore> makeCore() override;
    };

    class SimpleCore {
    private:
        function<unique_ptr<AggReducer>(DataRow & )> headerInit_;
        unique_ptr<AggReducer> reducer_;
    public:
        SimpleCore(function<unique_ptr<AggReducer>(DataRow & )> headerInit);

        void consume(DataRow &row);

        void reduce(SimpleCore &another);

        void dump(MemTable &table, function<bool(DataRow & )>);
    };

    class SimpleAgg : public Agg<SimpleCore>, public CoreMaker {
    public:
        SimpleAgg(const vector<uint32_t> &, function<vector<AggField *>()>);

    protected:
        unique_ptr<SimpleCore> makeCore() override;
    };
}
#endif //LQF_OPERATOR_AGG_H
