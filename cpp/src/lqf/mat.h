//
// Created by harper on 3/17/20.
//

#ifndef ARROW_MAT_H
#define ARROW_MAT_H

#include "data_model.h"
#include "parallel.h"
#include "rowcopy.h"

namespace lqf {

    using namespace parallel;
    using namespace rowcopy;

    /// Enable multi-load of filtered tables
    class FilterMat : public Node {
    public:
        FilterMat();

        virtual ~FilterMat() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> mat(Table &input);
    };

    class HashMat : public Node {
    private:
        uint32_t key_index_;
        unique_ptr<Snapshoter> snapshoter_;
    public:
        HashMat(uint32_t, unique_ptr<Snapshoter>);

        virtual ~HashMat() = default;

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> mat(Table &input);
    };

    class PowerHashMat : public Node {
    private:
        function<int64_t(DataRow &)> key_maker_;
        unique_ptr<Snapshoter> snapshoter_;
    public:
        PowerHashMat(function<int64_t(DataRow &)>, unique_ptr<Snapshoter>);

        unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override;

        shared_ptr<Table> mat(Table &input);
    };
}


#endif //ARROW_MAT_H
