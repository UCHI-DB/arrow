//
// Created by harper on 4/9/20.
//

#include "ssbquery.h"
#include "../env.h"
#include <chrono>
#include <iostream>

using namespace std;
using namespace std::chrono;
using namespace lqf;
using namespace lqf::ssb;

int main() {
// Get starting timepoint
    auto start = high_resolution_clock::now();

    // Call the function,
    env_init();
    executeQ1_1();
    env_cleanup();


    // Get ending timepoint
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    cout << "Time taken: " << duration.count() << " microseconds" << endl;
}