//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_AGG_H
#define LQF_OPERATOR_AGG_H

#include "data_model.h"

using namespace std;
using namespace std::placeholders;
namespace lqf {

    class AggField {
    protected:
        uint32_t readIndex_;
    public:
        AggField(uint32_t readIndex);

        virtual void reduce(DataRow &) = 0;

        virtual void dump(DataRow &, uint32_t) = 0;

        virtual void merge(AggField &) = 0;
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
            T value_;
        public:
            Sum(uint32_t index);

            void reduce(DataRow &) override;

            void dump(DataRow &, uint32_t) override;

            void merge(AggField &) override;
        };

        using IntSum = Sum<int32_t, AsInt>;
        using DoubleSum = Sum<double, AsDouble>;

        class Count : public AggField {
        protected:
            int32_t count_;
        public:
            Count();

            void reduce(DataRow &) override;

            void dump(DataRow &, uint32_t) override;

            void merge(AggField &) override;
        };

        template<typename T, typename ACC>
        class Max : public AggField {
        protected:
            T value_;
        public:
            Max(uint32_t rIndex);

            void reduce(DataRow &) override;

            void dump(DataRow &, uint32_t) override;

            void merge(AggField &) override;
        };

        using IntMax = Max<int32_t, AsInt>;
        using DoubleMax = Max<double, AsDouble>;

        template<typename T, typename ACC>
        class Min : public AggField {
        protected:
            T value_;
        public:
            Min(uint32_t rIndex);

            void reduce(DataRow &) override;

            void dump(DataRow &, uint32_t) override;

            void merge(AggField &) override;
        };

        using IntMin = Min<int32_t, AsInt>;
        using DoubleMin = Min<double, AsDouble>;

        template<typename T, typename ACC>
        class RecordingMin : public AggRecordingField {
        protected:
            T value_;
        public:
            RecordingMin(uint32_t vIndex, uint32_t kIndex);

            void reduce(DataRow &) override;

            void dump(DataRow &, uint32_t) override;

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

            void reduce(DataRow &) override;

            void dump(DataRow &, uint32_t) override;

            void merge(AggField &) override;
        };

        using IntAvg = Avg<int32_t, AsInt>;
        using DoubleAvg = Avg<double, AsDouble>;
    }

    class AggReducer {
    protected:
        MemDataRow header_;
        vector<unique_ptr<AggField>> fields_;
    public:
        AggReducer(uint32_t numHeaders, initializer_list<AggField *> fields);

        AggReducer(const vector<uint32_t> &col_size, initializer_list<AggField *> fields);

        MemDataRow &header();

        void reduce(DataRow &);

        void merge(AggReducer &reducer);

        virtual void dump(DataRowIterator &);

        virtual uint32_t size() { return 1; }
    };

    class AggRecordingReducer : public AggReducer {
    protected:
        unique_ptr<AggRecordingField> field_;
    public:

        AggRecordingReducer(uint32_t numHeaders, AggRecordingField *field);

        void dump(DataRowIterator &) override;

        uint32_t size() override;
    };

    template<typename CORE>
    class Agg {
    protected:
        vector<uint32_t> output_col_size_;
        bool vertical_;
        function<unique_ptr<CORE>()> coreMaker_;

        virtual unique_ptr<CORE> processBlock(const shared_ptr<Block> &block);

    public:
        Agg(uint32_t output_num_fields, function<unique_ptr<CORE>()> coreMaker, bool vertical = false);

        Agg(const vector<uint32_t> &output_col_size, function<unique_ptr<CORE>()> coreMaker, bool vertical = false);

        shared_ptr<Table> agg(Table &input);
    };

    template<typename CORE>
    class DictAgg : public Agg<CORE> {
    protected:
        vector<uint32_t> need_trans_;

        unique_ptr<CORE> processBlock(const shared_ptr<Block> &block) override;

    public:

        DictAgg(const vector<uint32_t> &, function<unique_ptr<CORE>()>, initializer_list<uint32_t>,
                bool vertical = false);
    };

    class HashCore {
    private:
        unordered_map<uint64_t, unique_ptr<AggReducer>> container_;
        function<uint64_t(DataRow &)> hasher_;
        function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
    public:
        HashCore(function<uint64_t(DataRow &)> hasher,
                 function<unique_ptr<AggReducer>(DataRow &)> headerInit);

        virtual ~HashCore();

        void consume(DataRow &row);

        void reduce(HashCore &another);

        void dump(MemTable &table);

        void translate(vector<uint32_t> &, function<void(DataField &, uint32_t, uint32_t)>);
    };

    using HashAgg = Agg<HashCore>;

    class TableCore {
    private:
        vector<unique_ptr<AggReducer>> container_;
        function<uint32_t(DataRow &)> indexer_;
        function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
    public:
        TableCore(uint32_t tableSize, function<uint32_t(DataRow &)> indexer,
                  function<unique_ptr<AggReducer>(DataRow &)> headerInit);

        virtual ~TableCore();

        void consume(DataRow &row);

        void reduce(TableCore &another);

        void dump(MemTable &table);

        void translate(vector<uint32_t> &, function<void(DataField &, uint32_t, uint32_t)>);
    };

    using TableAgg = Agg<TableCore>;


    class SimpleCore {
    private:
        function<unique_ptr<AggReducer>(DataRow &)> headerInit_;
        unique_ptr<AggReducer> reducer_;
    public:
        SimpleCore(function<unique_ptr<AggReducer>(DataRow &)> headerInit);

        void consume(DataRow &row);

        void reduce(SimpleCore &another);

        void dump(MemTable &table);

        void translate(vector<uint32_t> &, function<void(DataField &, uint32_t, uint32_t)>);
    };

    using SimpleAgg = Agg<SimpleCore>;
}
#endif //LQF_OPERATOR_AGG_H
