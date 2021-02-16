//
// Created by harper on 2/13/21.
//

#include "feature.h"
#include "immintrin.h"
#include <unordered_map>
#include <math.h>
#include <cstring>

namespace lqf {
    namespace encsel {
        using namespace std;

        void MemFeatureRecorder::Record(std::string name, double value) {

        }

        void Sparsity::Add(string data) {
            counter_ += 1;
            empty_counter_ += data.length() == 0;
        }

        void Sparsity::Close(FeatureRecorder &recorder) {
            recorder.Record("ratio", ((double) empty_counter_) / counter_);
        }

        Entropy::Entropy() {
            counter_ = (uint32_t *) aligned_alloc(64, sizeof(uint32_t) * 256);
            memset(counter_, 0, sizeof(uint32_t) * 256);
            line_counter_ = (uint32_t *) aligned_alloc(64, sizeof(uint32_t) * 256);
        }

        Entropy::~Entropy() {
            delete counter_;
            delete line_counter_;
        }

        void Entropy::Add(std::string data) {
            memset(line_counter_, 0, sizeof(uint32_t) * 256);
            for (char c: data) {
                counter_[(uint8_t) c] += 1;
                line_counter_[(uint8_t) c] += 1;
            }
            // Compute line entropy
            double sum = 0;
            for (int i = 0; i < 256; ++i) {
                sum += line_counter_[i];
            }
            double entropy = 0;
            for (int i = 0; i < 256; ++i) {
                double p = line_counter_[i] / sum;
                entropy += -p * log2(p);
            }
            line_entropy_.push_back(entropy);
        }

        void Entropy::Close(FeatureRecorder &recorder) {
            // Compute total entropy
            double sum = 0;
            for (int i = 0; i < 256; ++i) {
                sum += counter_[i];
            }
            double entropy = 0;
            for (int i = 0; i < 256; ++i) {
                double p = line_counter_[i] / sum;
                entropy += -p * log2(p);
            }

            recorder.Record("entropy", entropy);

            double lmax = -1;
            double lmin = INT32_MAX;
            double lsum = 0;
            for (auto &l: line_entropy_) {
                lmax = l > lmax ? l : lmax;
                lmin = l < lmin ? l : lmin;
                lsum += l;
            }
            double lmean = lsum / line_entropy_.size();
            double var = 0;
            for (auto &l : line_entropy_) {
                var += (l - lmean) * (l - lmean);
            }

            recorder.Record("lmax", lmax);
            recorder.Record("lmin", lmin);
            recorder.Record("lmean", lmin);
            recorder.Record("lvar", var);
        }

        void Length::Add(std::string data) {
            record_length_.push_back(data.length());
        }

        void Length::Close(FeatureRecorder &recorder) {
            uint32_t max = 0;
            uint32_t min = UINT32_MAX;
            double sum = 0;

            for (auto &l: record_length_) {
                max = l > max ? l : max;
                min = l < min ? l : min;
                sum += l;
            }
            double mean = sum / record_length_.size();
            double var = 0;
            for (auto &l : record_length_) {
                var += (l - mean) * (l - mean);
            }

            recorder.Record("max", max);
            recorder.Record("min", min);
            recorder.Record("mean", mean);
            recorder.Record("var", var);
        }

        void Distinct::Add(std::string data) {
            counter_++;
            dict_.insert(data);
        }

        void Distinct::Close(FeatureRecorder &recorder) {
            recorder.Record("ratio", (double) dict_.size() / counter_);
        }

        Sortness::Sortness(uint32_t window_size) : mt_rand_(time(0)), unif_(0.0, 1.0),
                                                   window_size_(window_size) {
            selection_ = 2.0 / window_size_;
            sample_current_ = unif_(mt_rand_) < selection_;
        }

        void Sortness::Add(std::string data) {
            if (sample_current_) {
                window_buffer_.push_back(stoi(data));
                if (window_buffer_.size() == window_size_) {
                    UpdateWindow();
                    window_buffer_.clear();
                    sample_current_ = unif_(mt_rand_) < selection_;
                }
            } else {
                window_counter_++;
                if (window_counter_ == window_size_) {
                    window_counter_ = 0;
                    sample_current_ = unif_(mt_rand_) < selection_;
                }
            }
        }

        void Sortness::UpdateWindow() {
            for (uint32_t i = 0; i < window_size_; ++i) {
                for (uint32_t j = i + 1; j < window_size_; ++j) {
                    if (window_buffer_[i] > window_buffer_[j])
                        inverted_pair_++;
                }
            }
            total_pair_ += window_size_ * (window_size_ - 1) / 2;

            vector<uint32_t> data;
            for (uint32_t i = 0; i < window_size_; ++i) {
                data.push_back(i);
            }
            sort(data.begin(), data.end(), *this);
            for (uint32_t i = 0; i < window_size_; ++i) {
                rankdiff_ += (i - data[i]) * (i - data[i]);
            }
        }

        bool Sortness::operator()(uint32_t a, uint32_t b) {
            return window_buffer_[a] < window_buffer_[b];
        }

        void Sortness::Close(FeatureRecorder &recorder) {
            double ratio = ((double) total_pair_ - inverted_pair_) / total_pair_;
            double ivpair = 1 - abs(2 * ratio - 1);
            // Kendall's Tau
            double ktau = ((double) total_pair_ - 2 * inverted_pair_) / total_pair_;
            // Spearman's Rho
            double srho = 1 - 6.0 * rankdiff_ / (counter_ * (counter_ * counter_ - 1));

            recorder.Record("ivpair", ivpair);
            recorder.Record("kendalltau", ktau);
            recorder.Record("spearmanrho", srho);
        }
    }
}