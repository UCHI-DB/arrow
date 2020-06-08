//
// Created by Harper on 5/28/20.
//

#include "../threadpool.h"
#include "../data_model.h"
#include "../join.h"
#include "../filter.h"
#include "tpchquery.h"
#include <iostream>

using namespace std;

using namespace lqf;
using namespace lqf::tpch;
using namespace lqf::hashcontainer;


class A {
public:
    virtual void print() = 0;
};

class B : public A {
public:
    void print() override {
        cout << "From B" << endl;
    }
};

class Builder {
protected:
    B b;
public:
    A &build() {
        return b;
    }
};

int main() {
    Builder builder;
    builder.build().print();

    A &a = builder.build();
    a.print();
}