//
// Created by harper on 3/15/18.
//

#include <cassert>
#include "unpacker.h"

namespace sboost {
    using namespace std;

    unique_ptr<Unpacker> Unpacker::Make(uint8_t bit_width) {
        Unpacker *unpacker;
        if (bit_width > 25 && bit_width < 32) {
            unpacker = new Large32Unpacker(bit_width);
        } else if (bit_width == 32) {
            unpacker = new TrivialUnpacker();
        } else {
            unpacker = new Small32Unpacker(bit_width);
        }
        return unique_ptr<Unpacker>(unpacker);
    }

    TrivialUnpacker::TrivialUnpacker() {

    }

    TrivialUnpacker::~TrivialUnpacker() {

    }

    __m256i TrivialUnpacker::unpack(const uint8_t *data, uint8_t offset) {
        return _mm256_loadu_si256((__m256i *) data);
    }


    Small32Unpacker::Small32Unpacker(uint32_t es) {
        this->entrySize = es;
        assert(es < 26);

        this->nextPos = new uint8_t[8];

        this->shuffleInst = (__m256i *) aligned_alloc(32, 32 * 8);
        this->shiftInst = (__m256i *) aligned_alloc(32, 32 * 8);

        this->mask = (__m256i *) aligned_alloc(32, 32);
        *mask = _mm256_set1_epi32((1 << es) - 1);

        __m128i *shuffleBuffer = (__m128i *) aligned_alloc(16, 16 * 8);
        __m128i *shiftBuffer = (__m128i *) aligned_alloc(16, 16 * 8);
        uint32_t *shuffleDataBuffer = (uint32_t *) aligned_alloc(16, 16);
        uint32_t *shiftDataBuffer = (uint32_t *) aligned_alloc(16, 16);

        // Compute shuffle and shift instructions
        for (int offset = 0; offset < 8; offset++) {
            for (int idx = 0; idx < 4; idx++) {
                uint32_t entryoff = offset + entrySize * idx;
                shiftDataBuffer[idx] = entryoff % 8;

                uint8_t round = (shiftDataBuffer[idx] + entrySize) / 8;
                shuffleDataBuffer[idx] = 0;
                int start = entryoff / 8;
                for (int bi = 0; bi <= round; bi++) {
                    shuffleDataBuffer[idx] |= (start + bi) << bi * 8;
                }
                for (int bi = round + 1; bi < 4; bi++) {
                    shuffleDataBuffer[idx] |= 0xff << bi * 8;
                }
            }
            nextPos[offset] = (offset + entrySize * 4) / 8;

            shuffleBuffer[offset] = _mm_load_si128((__m128i *) shuffleDataBuffer);
            shiftBuffer[offset] = _mm_load_si128((__m128i *) shiftDataBuffer);
        }

        // Combine them to make 256-bit shuffle and shift instructions
        for (int i = 0; i < 8; i++) {
            uint32_t higher = (i + entrySize * 4) % 8;
            this->shuffleInst[i] = _mm256_set_m128i(shuffleBuffer[higher], shuffleBuffer[i]);
            this->shiftInst[i] = _mm256_set_m128i(shiftBuffer[higher], shiftBuffer[i]);
        }

        free(shuffleDataBuffer);
        free(shiftDataBuffer);
        free(shuffleBuffer);
        free(shiftBuffer);
    }

    Small32Unpacker::~Small32Unpacker() {
        delete[] this->nextPos;
        free(shuffleInst);
        free(mask);
        free(shiftInst);
    }

    __m256i Small32Unpacker::unpack(const uint8_t *data, uint8_t offset) {
        // Load data into 2 128 buffer and combine as 256
        __m256i main = _mm256_loadu2_m128i((__m128i *) (data + nextPos[offset]), (__m128i *) data);
        // Shuffle
        __m256i shuffle = _mm256_shuffle_epi8(main, shuffleInst[offset]);
        // Shift
        __m256i shift = _mm256_srlv_epi32(shuffle, shiftInst[offset]);
        // Mask
        return _mm256_and_si256(shift, *mask);
    }


    Large32Unpacker::Large32Unpacker(uint32_t es) {
        this->entrySize = es;
        assert(es >= 26 && es < 32);

        nextPos = new uint8_t[24];

        shiftInst = (__m512i *) aligned_alloc(64, 8 * 64);
        shuffleInst = (__m512i *) aligned_alloc(64, 8 * 64);

        mask = (__m256i *) aligned_alloc(32, 32);
        *mask = _mm256_set1_epi32((1 << entrySize) - 1);

        __m128i *shuffleBuffer = (__m128i *) aligned_alloc(16, 16 * 8);
        __m128i *shiftBuffer = (__m128i *) aligned_alloc(16, 16 * 8);
        uint64_t *shuffleDataBuffer = (uint64_t *) aligned_alloc(16, 16);
        uint64_t *shiftDataBuffer = (uint64_t *) aligned_alloc(16, 16);

        // Compute shuffle and shift instructions
        for (int offset = 0; offset < 8; offset++) {
            for (int idx = 0; idx < 2; idx++) {
                uint32_t entryoff = offset + entrySize * idx;
                shiftDataBuffer[idx] = entryoff % 8;

                uint8_t round = (shiftDataBuffer[idx] + entrySize) / 8;
                shuffleDataBuffer[idx] = 0;
                uint64_t start = entryoff / 8;
                for (int bi = 0; bi <= round; bi++) {
                    shuffleDataBuffer[idx] |= (start + bi) << bi * 8;
                }
                for (int bi = round + 1; bi < 8; bi++) {
                    shuffleDataBuffer[idx] |= 0xffL << bi * 8;
                }
            }
            for (int j = 0; j < 3; j++) {
                nextPos[offset * 3 + j] = (offset + entrySize * 2 * (j + 1)) / 8;
            }

            shuffleBuffer[offset] = _mm_load_si128((__m128i *) shuffleDataBuffer);
            shiftBuffer[offset] = _mm_load_si128((__m128i *) shiftDataBuffer);
        }

        // Combine them to make 512-bit shuffle and shift instructions
        for (int i = 0; i < 8; i++) {
            int high = (i + entrySize * 2) % 8;
            int higher = (high + entrySize * 2) % 8;
            int evenHigher = (higher + entrySize * 2) % 8;

            __m128i su0 = shuffleBuffer[i];
            __m128i su1 = shuffleBuffer[high];
            __m128i su2 = shuffleBuffer[higher];
            __m128i su3 = shuffleBuffer[evenHigher];

            __m512i shuffle = _mm512_castsi128_si512(su0);
            shuffle = _mm512_inserti64x2(shuffle, su1, 1);
            shuffle = _mm512_inserti64x2(shuffle, su2, 2);
            shuffle = _mm512_inserti64x2(shuffle, su3, 3);
            this->shuffleInst[i] = shuffle;

            __m128i sh0 = shiftBuffer[i];
            __m128i sh1 = shiftBuffer[high];
            __m128i sh2 = shiftBuffer[higher];
            __m128i sh3 = shiftBuffer[evenHigher];

            __m512i shift = _mm512_castsi128_si512(sh0);
            shift = _mm512_inserti64x2(shift, sh1, 1);
            shift = _mm512_inserti64x2(shift, sh2, 2);
            shift = _mm512_inserti64x2(shift, sh3, 3);
            this->shiftInst[i] = shift;
        }

        free(shuffleDataBuffer);
        free(shiftDataBuffer);
        free(shuffleBuffer);
        free(shiftBuffer);
    }

    Large32Unpacker::~Large32Unpacker() {
        free(shiftInst);
        free(shuffleInst);
        free(mask);
        delete[] nextPos;
    }

    __m256i Large32Unpacker::unpack(const uint8_t *data, uint8_t offset) {
        // Load 4 128 bit into a 512 bit register
        __m256i lower = _mm256_loadu2_m128i((__m128i *) (data + nextPos[offset * 3]), (__m128i *) data);
        __m256i higher = _mm256_loadu2_m128i((__m128i *) (data + nextPos[offset * 3 + 2]),
                                             (__m128i *) (data + nextPos[offset * 3 + 1]));
        // Get a single 512 bit
        __m512i main = _mm512_castsi256_si512(lower);
        main = _mm512_inserti64x4(main, higher, 1);

        // Shuffle
        __m512i shuffle = _mm512_shuffle_epi8(main, shuffleInst[offset]);
        // Shift
        __m512i shift = _mm512_srlv_epi64(shuffle, shiftInst[offset]);
        // Mask
        return _mm256_and_si256(_mm512_cvtepi64_epi32(shift), *mask);
    }
}