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
#include <functional>
#include <memory>

#define _mm256_loadu2_m128i(vh, vl) _mm256_insertf128_si256(_mm256_castsi128_si256(_mm_loadu_si128((vl))), _mm_loadu_si128(vh), 1)
#define _mm256_set_m128i(vh, vl)  _mm256_insertf128_si256(_mm256_castsi128_si256(vl), (vh), 1)

using namespace std;
using namespace std::placeholders;
namespace sboost {

    class Unpacker {
    public:
        virtual __m256i unpack(const uint8_t *) = 0;
    };

    class Small32Unpacker : public Unpacker {
    private:
        uint32_t entrySize_;
        uint8_t nextPos_;
        __m256i *shuffleInst_;
        __m256i *shiftInst_;
        __m256i *mask_;
    public:
        Small32Unpacker(uint32_t es);

        virtual ~Small32Unpacker();

        __m256i unpack(const uint8_t *data) override;
    };

    class Large32Unpacker : public Unpacker {
    private:
        uint32_t entrySize_;
        std::array<uint32_t, 3> nextPos_;
        __m512i *shuffleInst_;
        __m512i *shiftInst_;
        __m256i *mask_;
    public:
        Large32Unpacker(uint32_t es);

        virtual ~Large32Unpacker();

        __m256i unpack(const uint8_t *data);
    };

    /**
     * Do not use inheritance for performance
     */
    static array<Unpacker *, 32> unpackers = {
            new Small32Unpacker(0), new Small32Unpacker(1), new Small32Unpacker(2),
            new Small32Unpacker(3), new Small32Unpacker(4), new Small32Unpacker(5),
            new Small32Unpacker(6), new Small32Unpacker(7), new Small32Unpacker(8),
            new Small32Unpacker(9), new Small32Unpacker(10), new Small32Unpacker(11),
            new Small32Unpacker(12), new Small32Unpacker(13), new Small32Unpacker(14),
            new Small32Unpacker(15), new Small32Unpacker(16), new Small32Unpacker(17),
            new Small32Unpacker(18), new Small32Unpacker(19), new Small32Unpacker(20),
            new Small32Unpacker(21), new Small32Unpacker(22), new Small32Unpacker(23),
            new Small32Unpacker(24), new Small32Unpacker(25), new Large32Unpacker(26),
            new Large32Unpacker(27), new Large32Unpacker(28), new Large32Unpacker(29),
            new Large32Unpacker(30), new Large32Unpacker(31)
    };

    void unpackScalar(const uint8_t *input, uint32_t numEntry, uint8_t bitWidth, uint32_t *output);

    template<typename UPCKR, int bitWidth>
    void unpack(const uint8_t *input, uint32_t numEntry, uint32_t *output) {
        UPCKR upckr(bitWidth);
        uint32_t round = numEntry >> 3;
        uint32_t ioff = 0;
        for (uint i = 0; i < round; ++i) {
            __m256i result = upckr.unpack(input + ioff);
            _mm256_storeu_si256(((__m256i *) output) + i, result);
            ioff += bitWidth;
        }
        unpackScalar(input + (round * bitWidth), numEntry & 0x7, bitWidth, output + (round << 3));
    }

    static std::array<function<void(const uint8_t *, uint32_t, uint32_t *)>, 32> unpacks =
            {unpack<Small32Unpacker, 0>, unpack<Small32Unpacker, 1>, unpack<Small32Unpacker, 2>,
             unpack<Small32Unpacker, 3>, unpack<Small32Unpacker, 4>, unpack<Small32Unpacker, 5>,
             unpack<Small32Unpacker, 6>, unpack<Small32Unpacker, 7>, unpack<Small32Unpacker, 8>,
             unpack<Small32Unpacker, 9>, unpack<Small32Unpacker, 10>, unpack<Small32Unpacker, 11>,
             unpack<Small32Unpacker, 12>, unpack<Small32Unpacker, 13>, unpack<Small32Unpacker, 14>,
             unpack<Small32Unpacker, 15>, unpack<Small32Unpacker, 16>, unpack<Small32Unpacker, 17>,
             unpack<Small32Unpacker, 18>, unpack<Small32Unpacker, 19>, unpack<Small32Unpacker, 20>,
             unpack<Small32Unpacker, 21>, unpack<Small32Unpacker, 22>, unpack<Small32Unpacker, 23>,
             unpack<Small32Unpacker, 24>, unpack<Small32Unpacker, 25>, unpack<Large32Unpacker, 26>,
             unpack<Large32Unpacker, 27>, unpack<Large32Unpacker, 28>, unpack<Large32Unpacker, 29>,
             unpack<Large32Unpacker, 30>, unpack<Large32Unpacker, 31>};
}
#endif //SBOOST_UNPACKER_H
