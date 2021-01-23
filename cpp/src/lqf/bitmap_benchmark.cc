//
// Created by harper on 4/27/20.
//
// Should we use dirty_ flag for Bitmap

#include <benchmark/benchmark.h>

static uint64_t bitmap_size = 10000000;

void useDirty(benchmark::State &state) {
    uint64_t *bitmap = (uint64_t *) malloc(sizeof(uint64_t) * bitmap_size);
    bool dirty_= false;
    for (auto _: state) {
        for(uint32_t i = 0 ; i < bitmap_size;++i) {
            auto pos = i;
            uint32_t index = static_cast<uint32_t>(pos >> 6);
            uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
            bitmap[index] |= 1L << offset;
            dirty_ = true;
        }
    }
    assert(dirty_);
    free(bitmap);
}

void noDirty(benchmark::State &state) {
    uint64_t *bitmap = (uint64_t *) malloc(sizeof(uint64_t) * bitmap_size);
    for (auto _: state) {
        for(uint32_t i = 0 ; i < bitmap_size;++i) {
            auto pos = i;
            uint32_t index = static_cast<uint32_t>(pos >> 6);
            uint32_t offset = static_cast<uint32_t> (pos & 0x3F);
            bitmap[index] |= 1L << offset;
        }
    }
    free(bitmap);
}

BENCHMARK(useDirty);
BENCHMARK(noDirty);
