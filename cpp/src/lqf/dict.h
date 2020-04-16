//
// Created by harper on 4/11/20.
//

#ifndef ARROW_DICT_H
#define ARROW_DICT_H

#include <memory>
#include <unordered_map>
#include <parquet/types.h>
#include <parquet/column_page.h>


namespace lqf {

    using namespace std;
    using namespace parquet;

    class DictionaryBase {
    protected:
        // We cache the page used by the dictionary here. Otherwise when it is released, byte array data will be lost.
        shared_ptr<DictionaryPage> page_;

        uint32_t size_;

        void *generic_buffer_;
    public:
        inline uint32_t size() {
            return size_;
        }
    };

    template<typename DTYPE>
    class Dictionary : public DictionaryBase {
    private:
        using T = typename DTYPE::c_type;

        T *buffer_;
    public:
        Dictionary();

        Dictionary(shared_ptr<DictionaryPage> data);

        Dictionary(void* buffer);

        virtual ~Dictionary();

        const T &operator[](int32_t key);

        int32_t lookup(const T &key);

        unique_ptr<vector<uint32_t>> list(function<bool(const T &)>);

        Dictionary<DTYPE> &operator=(Dictionary<DTYPE> &&other) {
            this->buffer_ = other.buffer_;
            other.buffer_ = nullptr;
            this->size_ = other.size_;
            return *this;
        }
    };

    using Int32Dictionary = Dictionary<Int32Type>;
    using DoubleDictionary = Dictionary<DoubleType>;
    using ByteArrayDictionary = Dictionary<ByteArrayType>;

}


#endif //ARROW_DICT_H
