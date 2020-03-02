//
// Created by harper on 3/15/18.
//

#ifndef SBOOST_UNPACKER_H
#define SBOOST_UNPACKER_H


/**
 * Implementations of unpacking integer into 256-bit SIMD
 */
#include <immintrin.h>
#include <cstdint>
#include <memory>

#define _mm256_loadu2_m128i(vh, vl) _mm256_insertf128_si256(_mm256_castsi128_si256(_mm_loadu_si128((vl))), _mm_loadu_si128(vh), 1)
#define _mm256_set_m128i(vh, vl)  _mm256_insertf128_si256(_mm256_castsi128_si256(vl), (vh), 1)

namespace sboost {
    using namespace std;

    class Unpacker {

    public:
        static unique_ptr<Unpacker> Make(uint8_t bit_width);

        Unpacker() {}

        virtual ~Unpacker() {}

        /**
         *
         * @param data byte buffer
         * @param offset offset in a byte
         * @return unpacked integer
         */
        virtual __m256i unpack(const uint8_t *data, uint8_t offset) = 0;
    };


    class TrivialUnpacker : public Unpacker {
    public:
        TrivialUnpacker();

        virtual ~TrivialUnpacker();


        __m256i unpack(const uint8_t *data, uint8_t offset);
    };

    class Small32Unpacker : public Unpacker {
    private:
        uint32_t entrySize;

        uint8_t *nextPos;

        __m256i *shuffleInst;
        __m256i *shiftInst;
        __m256i *mask;
    public:
        Small32Unpacker(uint32_t es);

        virtual ~Small32Unpacker();

        __m256i unpack(const uint8_t *data, uint8_t offset);
    };


    class Large32Unpacker : public Unpacker {
    private:
        uint32_t entrySize;

        uint8_t *nextPos;

        __m512i *shuffleInst;
        __m512i *shiftInst;

        __m256i *mask;
    public:
        Large32Unpacker(uint32_t es);

        virtual ~Large32Unpacker();

        __m256i unpack(const uint8_t *data, uint8_t offset);
    };

}
#endif //SBOOST_UNPACKER_H
