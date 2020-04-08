//
// Created by harper on 3/17/20.
//

#ifndef ARROW_MAT_H
#define ARROW_MAT_H

#include <lqf/data_model.h>

#define MLB [](DataRow& in, DataRow& out) {
#define MI(i, j) out[j] = in[i].asInt();
#define MD(i, j) out[j] = in[i].asDouble();
#define MB(i, j) out[j] = in[i].asByteArray();
#define MLE }

namespace lqf {

    class MemMat {
    public:
        MemMat(uint32_t, function<void(DataRow & , DataRow & )>);

        shared_ptr<MemTable> mat(Table &input);

    protected:
        uint32_t num_fields_;
        function<void(DataRow & , DataRow & )> loader_;

        void matBlock(MemTable *table, const shared_ptr<Block> &);
    };

    /// Enable multi-load of filtered tables

    class FilterMat {
    public:
        shared_ptr<Table> mat(Table &input);
    };

    class HashMat {
    private:
        uint32_t key_index_;
        function<unique_ptr<MemDataRow>(DataRow & )> snapshoter_;
    public:
        HashMat(uint32_t, function<unique_ptr<MemDataRow>(DataRow & )>);

        shared_ptr<Table> mat(Table &input);
    };

    class PowerHashMat {
    private:
        function<int64_t(DataRow & )> key_maker_;
        function<unique_ptr<MemDataRow>(DataRow & )> snapshoter_;
    public:
        PowerHashMat(function<int64_t(DataRow & )>, function<unique_ptr<MemDataRow>(DataRow & )>);

        shared_ptr<Table> mat(Table &input);
    };
}


#endif //ARROW_MAT_H
