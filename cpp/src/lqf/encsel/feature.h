//
// Created by harper on 2/13/21.
//

#ifndef ENCSEL_FEATURE_H
#define ENCSEL_FEATURE_H

#include <cstdint>
#include <ctime>
#include <string>
#include <vector>
#include <random>
#include <sparsehash/dense_hash_set>

namespace lqf {
    namespace encsel {

        class FeatureRecorder {
        public:
            virtual void Record(std::string name, double value) = 0;
        };

        class MemFeatureRecorder : public FeatureRecorder {
        public:
            void Record(std::string name, double value) override;
        };

        class Feature {
        public:
            virtual void Add(std::string data) = 0;

            virtual void Close(FeatureRecorder &) = 0;
        };

        class Sparsity : public Feature {
        protected:
            uint32_t counter_;
            uint32_t empty_counter_;
        public:
            void Add(std::string data) override;

            void Close(FeatureRecorder &) override;
        };

        class Entropy : public Feature {
        protected:
            uint32_t *line_counter_;
            uint32_t *counter_;
            std::vector<double> line_entropy_;
        public:
            Entropy();

            virtual ~Entropy();

            void Add(std::string data) override;

            void Close(FeatureRecorder &) override;
        };

        class Length : public Feature {
        protected:
            std::vector<uint32_t> record_length_;
        public:
            void Add(std::string data) override;

            void Close(FeatureRecorder &) override;
        };

        class Distinct : public Feature {
        protected:
            uint32_t counter_;
            google::dense_hash_set<std::string> dict_;
        public:
            void Add(std::string data) override;

            void Close(FeatureRecorder &) override;
        };

        /**
         * Sortness for integer
         */
        class Sortness : public Feature {
        protected:
            double selection_;
            std::mt19937 mt_rand_;
            std::uniform_real_distribution<double> unif_;
            bool sample_current_ = true;
            uint32_t window_size_;
            uint32_t window_counter_ = 0;
            std::vector<int32_t> window_buffer_;

            uint32_t total_pair_ = 0;
            uint32_t inverted_pair_ = 0;
            uint32_t counter_ = 0;
            uint32_t rankdiff_ = 0;

            void UpdateWindow();
        public:
            Sortness(uint32_t window_size);

            void Add(std::string data) override;

            void Close(FeatureRecorder &) override;

            // Comparator for sort
            bool operator()(uint32_t, uint32_t);
        };
    }
}


#endif //ENCSEL_FEATURE_H
