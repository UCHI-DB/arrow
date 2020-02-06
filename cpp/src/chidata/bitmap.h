//
// Created by harper on 2/5/20.
//

#ifndef CHIDATA_BITMAP_H
#define CHIDATA_BITMAP_H

#include <cstdint>

namespace chidata {

    class BitmapIterator {
    public:
        virtual ~BitmapIterator() = 0;

        virtual void move_to(uint64_t pos) = 0;

        virtual bool has_next() = 0;

        virtual uint64_t next() = 0;
    };

    class SimpleBitmapIterator : public BitmapIterator {
    private:
        uint64_t *content_;
        uint64_t content_size_;
        uint64_t num_bits_;
        uint64_t pointer_;
        uint64_t cached_;
        uint64_t final_mask_;
    public :
        SimpleBitmapIterator(uint64_t *content, uint64_t content_size, uint64_t bit_size);

        virtual ~SimpleBitmapIterator();

        virtual void move_to(uint64_t pos) override;

        virtual bool has_next() override;

        virtual uint64_t next() override;
    };

    class FullBitmapIterator : public BitmapIterator {
    private:
        uint64_t size_;
        uint64_t counter_;
    public :
        FullBitmapIterator(uint64_t size);

        virtual ~FullBitmapIterator();

        virtual void move_to(uint64_t pos) override;

        virtual bool has_next() override;

        virtual uint64_t next() override;
    };

    class Bitmap {

    public:
        virtual bool check(uint64_t pos) = 0;

        virtual void put(uint64_t pos) = 0;

        virtual void clear() = 0;

        virtual Bitmap *land(Bitmap *x1) = 0;

        virtual Bitmap *lor(Bitmap *x1) = 0;

        virtual uint64_t cardinality() = 0;

        virtual uint64_t size() = 0;

        virtual bool is_full() = 0;

        virtual bool is_empty() = 0;

        virtual double ratio() = 0;

        virtual BitmapIterator *iterator() = 0;

        virtual Bitmap *clone() {
            return this;
        };
    };

    class SimpleBitmap : public Bitmap {
    private:
        uint64_t *bitmap_;
        uint64_t array_size_;
        uint64_t size_;
        uint64_t first_valid_ = -1;
    public:
        SimpleBitmap(uint64_t size);

        virtual ~SimpleBitmap();

        virtual bool check(uint64_t pos) override;

        virtual void put(uint64_t pos) override;

        virtual void clear() override;

        virtual Bitmap *land(Bitmap *x1) override;

        virtual Bitmap *lor(Bitmap *x1) override;

        virtual uint64_t cardinality() override;

        virtual uint64_t size() override;

        virtual bool is_full() override;

        virtual bool is_empty() override;

        virtual double ratio() override;

        virtual BitmapIterator *iterator() override;
    };

    class FullBitmap : public Bitmap {
    public:
        FullBitmap(uint64_t size);

        virtual bool check(uint64_t pos) override;

        virtual void put(uint64_t pos) override;

        virtual void clear() override;

        virtual Bitmap *land(Bitmap *x1) override;

        virtual Bitmap *lor(Bitmap *x1) override;

        virtual uint64_t cardinality() override;

        virtual uint64_t size() override;

        virtual bool is_full() override;

        virtual bool is_empty() override;

        virtual double ratio() override;

        virtual BitmapIterator *iterator() override;

    private:
        uint64_t size_;
    };
}
#endif //CHIDATA_BITMAP_H
