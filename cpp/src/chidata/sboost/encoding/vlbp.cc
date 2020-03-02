//
// Created by harper on 10/21/19.
//

#include "vlbp.h"
#include "sboost/byteutils.h"
#include "sboost/sboost.h"
#include "sboost/encoding/encoding_utils.h"
#include "sboost/bitmap_writer.h"

#define GROUP_SIZE 512

namespace sboost {
    namespace encoding {
        namespace vlbp {

            template<typename PROC>
            void process(const uint8_t *input, uint64_t *output, uint32_t outputOffset,
                         uint32_t numEntry, PROC &proc) {
                uint32_t inputOffset = 0;
                byteutils::readIntLittleEndian(input, &inputOffset);

                uint32_t count = 0;
                BitmapWriter bitmap(output, outputOffset);

                while (count + GROUP_SIZE <= numEntry) {
                    uint8_t bitWidth = input[inputOffset++];
                    int32_t groupBase = byteutils::readZigZagVarInt(input, &inputOffset);

                    proc.apply(input + inputOffset, GROUP_SIZE, groupBase, bitWidth, bitmap);
                    inputOffset += 64 * bitWidth;
                    count += GROUP_SIZE;
                }
                // Process the last group
                if (count < numEntry) {
                    uint32_t remain = numEntry - count;
                    uint8_t bitWidth = input[inputOffset++];
                    int32_t groupBase = byteutils::readZigZagVarInt(input, &inputOffset);

                    proc.apply(input + inputOffset, remain, groupBase, bitWidth, bitmap);
                    inputOffset += ((numEntry - count) * bitWidth) >> 3;
                    // Round to the smallest 64 multiple
                    count = ((numEntry + 63) >> 6) << 6;
                }

                cleanup(count, numEntry, output, outputOffset);
            }

            class EqualProc {
            private:
                uint32_t value_;
            public:
                EqualProc(uint32_t value) : value_(value) {};

                void
                apply(const uint8_t *data, uint32_t size, uint32_t groupBase, uint32_t bitWidth, BitmapWriter &bitmap) {
                    uint32_t expectBitWidth = 32 - __builtin_clz(value_ - groupBase);

                    if (value_ < groupBase || expectBitWidth > bitWidth) {
                        bitmap.appendBits(0, size);
                    } else {
                        sboost::Bitpack sboost(bitWidth, value_ - groupBase);
                        sboost.equal(data, size, bitmap.base(), bitmap.offset());
                        bitmap.moveForward(size);
                    }
                }
            };

            void equal(const uint8_t *input, uint64_t *output, uint32_t outputOffset,
                       uint32_t numEntry, uint32_t value) {
                EqualProc proc(value);
                process<EqualProc>(input, output, outputOffset, numEntry, proc);
            }

            class LessProc {
            private:
                uint32_t value_;
            public:
                LessProc(uint32_t value) : value_(value) {};

                void
                apply(const uint8_t *data, uint32_t size, uint32_t groupBase, uint32_t bitWidth, BitmapWriter &bitmap) {
                    uint32_t expectBitWidth = 32 - __builtin_clz(value_ - groupBase);

                    if (value_ < groupBase) {
                        // All not match
                        bitmap.appendBits(0, size);
                    } else if (expectBitWidth > bitWidth) {
                        // All match, fill in 64 bits.
                        bitmap.appendBits(1, size);
                    } else {
                        sboost::Bitpack sboost(bitWidth, value_ - groupBase);
                        sboost.less(data, size, bitmap.base(), bitmap.offset());
                        bitmap.moveForward(size);
                    }
                }
            };

            void less(const uint8_t *input, uint64_t *output, uint32_t outputOffset,
                      uint32_t numEntry, uint32_t value) {
                LessProc proc(value);
                process<LessProc>(input, output, outputOffset, numEntry, proc);
            }

            class GreaterProc {
            private:
                uint32_t value_;
            public:
                GreaterProc(uint32_t value) : value_(value) {};

                void
                apply(const uint8_t *data, uint32_t size, uint32_t groupBase, uint32_t bitWidth, BitmapWriter &bitmap) {
                    uint32_t expectBitWidth = 32 - __builtin_clz(value_ - groupBase);

                    if (value_ < groupBase) {
                        // All match
                        bitmap.appendBits(1, size);
                    } else if (expectBitWidth > bitWidth) {
                        // All not match.
                        bitmap.appendBits(0, size);
                    } else {
                        sboost::Bitpack sboost(bitWidth, value_ - groupBase);
                        sboost.greater(data, size, bitmap.base(), bitmap.offset());
                        bitmap.moveForward(size);
                    }
                }
            };

            void greater(const uint8_t *input, uint64_t *output, uint32_t outputOffset,
                         uint32_t numEntry, uint32_t value) {
                GreaterProc proc(value);
                process<GreaterProc>(input, output, outputOffset, numEntry, proc);
            }

            class RangeleProc {
            private:
                uint32_t lower_;
                uint32_t upper_;
            public:
                RangeleProc(uint32_t l, uint32_t u) : lower_(l), upper_(u) {};

                void
                apply(const uint8_t *data, uint32_t size, uint32_t groupBase, uint32_t bitWidth, BitmapWriter &bitmap) {
                    uint32_t lowerBitWidth = 32 - __builtin_clz(lower_ - groupBase);
                    uint32_t upperBitWidth = 32 - __builtin_clz(upper_ - groupBase);

                    if (lower_ < groupBase) {
                        if (upperBitWidth > bitWidth) {
                            bitmap.appendBits(1, size);
                        } else {
                            sboost::Bitpack sboost(bitWidth, upper_ - groupBase);
                            sboost.less(data, size, bitmap.base(), bitmap.offset());
                            bitmap.moveForward(size);
                        }
                    } else {
                        if (lowerBitWidth > bitWidth) {
                            bitmap.appendBits(0, size);
                        } else if (upperBitWidth > bitWidth) {
                            sboost::Bitpack sboost(bitWidth, lower_ - groupBase);
                            sboost.geq(data, size, bitmap.base(), bitmap.offset());
                            bitmap.moveForward(size);
                        } else {
                            sboost::Bitpack sboost(bitWidth, lower_ - groupBase, upper_ - groupBase);
                            sboost.rangele(data, size, bitmap.base(), bitmap.offset());
                            bitmap.moveForward(size);
                        }
                    }
                }
            };

            void rangele(const uint8_t *input, uint64_t *output, uint32_t outputOffset,
                         uint32_t numEntry, uint32_t lower, uint32_t upper) {
                RangeleProc proc(lower, upper);
                process<RangeleProc>(input, output, outputOffset, numEntry, proc);
            }

            class BetweenProc {
            private:
                uint32_t lower_;
                uint32_t upper_;
            public:
                BetweenProc(uint32_t l, uint32_t u) : lower_(l), upper_(u) {};

                void
                apply(const uint8_t *data, uint32_t size, uint32_t groupBase, uint32_t bitWidth, BitmapWriter &bitmap) {
                    uint32_t lowerBitWidth = 32 - __builtin_clz(lower_ - groupBase);
                    uint32_t upperBitWidth = 32 - __builtin_clz(upper_ - groupBase);

                    if (lower_ < groupBase) {
                        if (upperBitWidth > bitWidth) {
                            bitmap.appendBits(1, size);
                        } else {
                            sboost::Bitpack sboost(bitWidth, upper_ - groupBase);
                            sboost.leq(data, size, bitmap.base(), bitmap.offset());
                            bitmap.moveForward(size);
                        }
                    } else {
                        if (lowerBitWidth > bitWidth) {
                            bitmap.appendBits(0, size);
                        } else if (upperBitWidth > bitWidth) {
                            sboost::Bitpack sboost(bitWidth, lower_ - groupBase);
                            sboost.geq(data, size, bitmap.base(), bitmap.offset());
                            bitmap.moveForward(size);
                        } else {
                            sboost::Bitpack sboost(bitWidth, lower_ - groupBase, upper_ - groupBase);
                            sboost.between(data, size, bitmap.base(), bitmap.offset());
                            bitmap.moveForward(size);
                        }
                    }
                }
            };

            void between(const uint8_t *input, uint64_t *output, uint32_t outputOffset,
                         uint32_t numEntry, uint32_t lower, uint32_t upper) {
                BetweenProc proc(lower, upper);
                process<BetweenProc>(input, output, outputOffset, numEntry, proc);
            }
        }
    }
}
