//
// Created by Harper on 12/28/20.
//

#include <iostream>
#include <cuckoohash_map.hh>
#include <sparsehash/dense_hash_map>

using namespace std;
using namespace libcuckoo;
using namespace google;

int main() {

    dense_hash_map<int32_t, int32_t> map;
    map.set_empty_key(-2);

    for (int i = 0; i < 10000; ++i) {
        map[i] = i * 5;
    }

    for (int i = 0; i < 10010; ++i) {
        auto result = map.find(i);
        if (result != map.end())
            cout << result->second << '\n';
        else
            cout << "Not found" << '\n';
    }
}