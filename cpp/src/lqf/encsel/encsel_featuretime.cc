//
// Created by harper on 2/13/21.
//

#include <chrono>
#include <iostream>
#include "feature.h"
#include <vector>

using namespace std;
using namespace std::chrono;
using namespace lqf::encsel;

int main() {

    std::vector<unique_ptr<Feature>> features;
    features.push_back(unique_ptr<Feature>(new Sparsity()));
    features.push_back(unique_ptr<Feature>(new Entropy()));
    features.push_back(unique_ptr<Feature>(new Length()));
    features.push_back(unique_ptr<Feature>(new Distinct()));
    features.push_back(unique_ptr<Feature>(new Sortness(50)));

    MemFeatureRecorder recorder;

    auto start = high_resolution_clock::now();

    for (int i = 0; i < 10000; ++i) {
        auto s = to_string(i);
        for (auto &f: features) {
            f->Add(s);
        }
    }
    for (auto &f: features) {
        f->Close(recorder);
    }

    // Get ending timepoint
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start);
    cout << "Time taken: " << duration.count() << " microseconds" << endl;
}