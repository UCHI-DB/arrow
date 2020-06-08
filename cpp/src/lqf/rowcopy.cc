//
// Created by Harper on 6/6/20.
//

#include "rowcopy.h"

namespace lqf {
    namespace rowcopy {

        INPUT_TYPE table_type(Table &table) {
            if (table.isExternal()) {
                return I_EXTERNAL;
            }
            auto mtable = dynamic_cast<MemTable *>(&table);
            if (mtable && !mtable->isVertical()) {
                return I_RAW;
            }
            return I_OTHER;
        }

        RowCopyFactory *RowCopyFactory::from(INPUT_TYPE from) {
            from_type_ = from;
            return this;
        }

        RowCopyFactory *RowCopyFactory::to(INPUT_TYPE to) {
            to_type_ = to;
            return this;
        }

        RowCopyFactory *RowCopyFactory::from_layout(const vector<uint32_t> &offset) {
            from_offset_ = offset;
            return this;
        }

        RowCopyFactory *RowCopyFactory::to_layout(const vector<uint32_t> &offset) {
            to_offset_ = offset;
            return this;
        }

        FieldInst::FieldInst(FIELD_TYPE t, uint32_t f, uint32_t to)
                : type_(t), from_(f), to_(to) {}

        RowCopyFactory *RowCopyFactory::field(FIELD_TYPE type, uint32_t from, uint32_t to) {
            fields_.emplace_back(type, from, to);
            return this;
        }

        class RowCopier {
        protected:
            vector<function<void(DataRow &, DataRow &)>> contents_;
        public:

            RowCopier() {}

            RowCopier(RowCopier &from) : contents_(from.contents_) {}

            RowCopier(RowCopier &&from) : contents_(move(from.contents_)) {}

            virtual ~RowCopier() = default;

            void add(function<void(DataRow &, DataRow &)> f) {
                contents_.push_back(f);
            }

            void operator()(DataRow &to, DataRow &from) {
                for (auto &p:contents_) {
                    p(to, from);
                }
            }
        };

        unique_ptr<function<void(DataRow &, DataRow &)> > RowCopyFactory::build() {
            RowCopier rc;
            if (!fields_.empty()) {
                if (from_type_ == I_RAW && to_type_ == I_RAW
                    && !from_offset_.empty() && !to_offset_.empty()) {
                    // Compute consecutive sections
                    std::sort(fields_.begin(), fields_.end(),
                              [](FieldInst &a, FieldInst &b) { return a.from_ < b.from_; });

                    uint32_t from_start = -1;
                    uint32_t to_start = -1;
                    uint32_t length = 0;
                    uint32_t f_prev = -1;
                    uint32_t t_prev = -1;
                    for (auto &next: fields_) {
                        if (from_start == 0xFFFFFFFF) {
                            from_start = from_offset_[next.from_];
                            to_start = to_offset_[next.to_];
                            length = from_offset_[next.from_ + 1] - from_start;
                        } else if (next.from_ == f_prev + 1 && next.to_ == t_prev + 1) {
                            length += from_offset_[next.from_ + 1] - from_offset_[next.from_];
                        } else {
                            rc.add(bind(elements::rc_fieldmemcpy,
                                        placeholders::_1, placeholders::_2, from_start, to_start, length));
                            from_start = from_offset_[next.from_];
                            to_start = to_offset_[next.to_];
                            length = from_offset_[next.from_ + 1] - from_start;
                        }
                        f_prev = next.from_;
                        t_prev = next.to_;
                    }
                    rc.add(bind(elements::rc_fieldmemcpy,
                                placeholders::_1, placeholders::_2, from_start, to_start, length));
                } else {
                    for (auto &field: fields_) {
                        switch (field.type_) {
                            case F_REGULAR:
                                rc.add(bind(elements::rc_field, placeholders::_1, placeholders::_2,
                                            field.to_, field.from_));
                                break;
                            case F_STRING:
                                rc.add(bind(from_type_ == I_EXTERNAL ? elements::rc_extfield : elements::rc_field,
                                            placeholders::_1, placeholders::_2,
                                            field.to_, field.from_));
                                break;
                            case F_RAW:
                                rc.add(bind(elements::rc_rawfield, placeholders::_1, placeholders::_2,
                                            field.to_, field.from_));
                                break;
                        }
                    }
                }
            }
            return unique_ptr<function<void(DataRow &, DataRow &)>>(new function<void(DataRow &, DataRow &)>(move(rc)));
        }

        unique_ptr<function<void(DataRow &, DataRow &)> >
        RowCopyFactory::buildAssign(INPUT_TYPE from, INPUT_TYPE to, uint32_t num_fields) {
            this->from(from);
            this->to(to);
            auto offset = colOffset(num_fields);
            this->from_layout(offset);
            this->to_layout(offset);
            for (auto i = 0u; i < num_fields; ++i) {
                this->field(F_REGULAR, i, i);
            }
            return this->build();
        }

        unique_ptr<function<void(DataRow &, DataRow &)> >
        RowCopyFactory::buildAssign(INPUT_TYPE from, INPUT_TYPE to, vector<uint32_t> &col_offset) {
            this->from(from);
            this->to(to);
            this->from_layout(col_offset);
            this->to_layout(col_offset);
            for (auto i = 0u; i < col_offset.size() - 1; ++i) {
                this->field(col_offset[i + 1] - col_offset[i] == 2 ? F_STRING : F_REGULAR, i, i);
            }
            return this->build();
        }
    }
}