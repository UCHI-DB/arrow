//
// Created by harper on 2/21/20.
//

#ifndef LQF_OPERATOR_FILTER_H
#define LQF_OPERATOR_FILTER_H

#include <memory>
#include <chidata/lqf/data_model.h>
#include <chidata/bitmap.h>

using namespace std;

namespace chidata {
    namespace lqf {

        class Filter {
        protected:
            shared_ptr<Block> processBlock(const shared_ptr<Block> &);

            virtual shared_ptr<Bitmap> filterBlock(Block &) = 0;

        public:
            shared_ptr<Table> filter(Table &input);
        };

        class ColPredicate {
        public:
            virtual shared_ptr<Bitmap> filter(Block &, Bitmap &) = 0;
        };

        class SBoostPredicate {
        public:
            virtual shared_ptr<Bitmap> filter(Block &, Bitmap &) = 0;
        };

        class SimpleColPredicate : ColPredicate {
        private:
            uint32_t index_;
            function<bool(DataField &)> pred_;
        public:
            SimpleColPredicate(uint32_t index, function<bool(DataField &)> pred) : index_(index), pred_(pred) {};

            ~SimpleColPredicate() {}

            shared_ptr<Bitmap> filter(Block &, Bitmap &) override;
        };

        class ColFilter : public Filter {
        protected:
            vector<unique_ptr<ColPredicate>> predicates_;

            virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

        public:
            ColFilter();

            virtual ~ColFilter();

            void install(unique_ptr<ColPredicate> pred);
        };

        class RowFilter : public Filter {
        private:
            function<bool(DataRow &)> predicate_;

            virtual shared_ptr<Bitmap> filterBlock(Block &input) override;

        public:
            RowFilter(function<bool(DataRow &)> pred);

            virtual ~RowFilter() {}
        };
    }
}
#endif //LQF_OPERATOR_FILTER_H
