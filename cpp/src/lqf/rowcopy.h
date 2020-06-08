//
// Created by Harper on 6/6/20.
//

#ifndef LQF_ROW_COPY_H
#define LQF_ROW_COPY_H

#include <memory>
#include <initializer_list>
#include <array>
#include "data_model.h"
#include "memorypool.h"

namespace lqf {
    namespace rowcopy {

        using namespace std;

        enum INPUT_TYPE {
            I_RAW, I_EXTERNAL, I_OTHER
        };

        INPUT_TYPE table_type(Table &table);

        enum FIELD_TYPE {
            F_REGULAR, F_STRING, F_RAW
        };

        struct FieldInst {
            FIELD_TYPE type_;
            uint32_t from_;
            uint32_t to_;

            FieldInst(FIELD_TYPE, uint32_t, uint32_t);
        };

        /**
         * Generate
         */
        class RowCopyFactory {
        protected:
            INPUT_TYPE from_type_;
            INPUT_TYPE to_type_;
            vector<uint32_t> from_offset_;
            vector<uint32_t> to_offset_;
            vector<FieldInst> fields_;
        public:
            RowCopyFactory *from(INPUT_TYPE from);

            RowCopyFactory *to(INPUT_TYPE to);

            RowCopyFactory *from_layout(const vector<uint32_t> &offset);

            RowCopyFactory *to_layout(const vector<uint32_t> &offset);

            RowCopyFactory *field(FIELD_TYPE type, uint32_t from, uint32_t to);

            unique_ptr<function<void(DataRow &, DataRow &)>> build();

            unique_ptr<function<void(DataRow &, DataRow &)>>
            buildAssign(INPUT_TYPE from, INPUT_TYPE to, uint32_t num_fields);

            unique_ptr<function<void(DataRow &, DataRow &)>>
            buildAssign(INPUT_TYPE from, INPUT_TYPE to, vector<uint32_t> &col_size);
        };

        namespace elements {

            inline void rc_memcpy(DataRow &to, DataRow &from) {
                memcpy(to.raw(), from.raw(), to.size() * sizeof(uint64_t));
            }

            inline void
            rc_fieldmemcpy(DataRow &to, DataRow &from, uint32_t from_start, uint32_t to_start, uint32_t len) {
                memcpy((void*)(to.raw() + to_start), (void*)(from.raw() + from_start), len * sizeof(uint64_t));
            }

            inline void rc_field(DataRow &to, DataRow &from, uint32_t to_idx, uint32_t from_idx) {
                to[to_idx] = from[from_idx];
            }

            inline void rc_extfield(DataRow &to, DataRow &from, uint32_t to_idx, uint32_t from_idx) {
                DataField &tofield = to[to_idx];
                tofield = from[from_idx];
                memory::ByteArrayBuffer::instance.allocate(tofield.asByteArray());
            }

            inline void rc_rawfield(DataRow &to, DataRow &from, uint32_t to_idx, uint32_t from_idx) {
                to[to_idx] = from(from_idx);
            }
        }
    }
}


#endif //ARROW_ROW_COPY_H
