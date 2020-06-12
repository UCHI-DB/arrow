//
// Created by harper on 9/30/19.
//

#ifndef SBOOST_SBOOST_H
#define SBOOST_SBOOST_H

#include <cstdint>
#include <functional>
#include <immintrin.h>
#include "unpacker.h"

namespace sboost {

    class Bitpack {
    protected:
        uint32_t bitWidth;
        uint32_t target;
        uint32_t target2;
        uint64_t extract;

        __m512i mask;
        __m512i msbmask;
        __m512i spanned;
        __m512i nspanned;
        __m512i l2;
        __m512i g2;

        __m512i spanned2;
        __m512i nspanned2;
        __m512i l22;
        __m512i g22;
    public:
        Bitpack(uint32_t bitWidth, uint32_t target);

        Bitpack(uint32_t bitWidth, uint32_t t1, uint32_t t2);

        virtual ~Bitpack();

        void equal(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);

        void less(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);

        void leq(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);

        void greater(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);

        void geq(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);

        void rangele(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);

        void between(const uint8_t *data, uint32_t numEntry, uint64_t *res, uint32_t resoffset);
    };

    class BitpackCompare {
    protected:
        uint32_t bit_width_;
        uint64_t extract_;

        __m512i mask_;
        __m512i msbmask_;
    public:
        BitpackCompare(uint32_t bitWidth);

        virtual ~BitpackCompare() = default;

        void less(const uint8_t *left, const uint8_t *right, uint32_t numEntry, uint64_t *res, uint32_t resoffset);
    };

    using namespace std;


    __m256i cumsum32(__m256i b);
}
#endif //SBOOST_SBOOST_H
