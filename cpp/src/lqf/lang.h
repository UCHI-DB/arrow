//
// Created by harper on 2/15/20.
//

#ifndef CHIDATA_LANG_H
#define CHIDATA_LANG_H

namespace lqf {

    template<typename type>
    class Iterator {
    public:
        virtual bool hasNext() = 0;

        virtual type next() = 0;
    };
}

#endif //ARROW_LANG_H
