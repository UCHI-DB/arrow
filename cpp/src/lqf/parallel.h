//
// Created by Harper on 4/29/20.
//

#ifndef LQF_PARALLEL_H
#define LQF_PARALLEL_H

#include <cstdint>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <initializer_list>
#include "threadpool.h"

namespace lqf {
    using namespace threadpool;

    namespace parallel {

        ///
        /// Wrapper for the output from node
        ///
        class NodeOutput {
        };

        template<typename T>
        class TypedOutput : public NodeOutput {
            const T output_;
        public:
            TypedOutput(const T output) : output_(move(output)) {};

            const T get() { return output_; }
        };

        class ExecutionGraph;

        ///
        /// A node represents an execution unit
        ///
        class Node {
            friend ExecutionGraph;
        protected:
            // index in the execution graph
            uint32_t index_;
            uint32_t num_input_;
            atomic<uint32_t> ready_counter_;
            // Flag that this node has no heavy computation and does not block execution
            bool trivial_;
            bool triggered_ = false;
        public:
            Node(const uint32_t num_input, const bool trivial = false);

            // One of its ancestor is ready
            bool feed();

            inline bool trivial() { return trivial_; };

            virtual unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) = 0;
        };

        template<typename T>
        class WrapperNode : public Node {
        protected:
            T content_;
        public:
            WrapperNode(T content) : Node(0, true), content_(move(content)) {}

            unique_ptr<NodeOutput> execute(const vector<NodeOutput *> &) override {
                return unique_ptr<NodeOutput>(new TypedOutput<T>(content_));
            }
        };

        // Add custom condition before and after a node execution
        class NestedNode : public Node {
        protected:
            unique_ptr<Node> inner_;
        public:
            NestedNode(Node *inner, uint32_t num_input, bool trivial = false)
                    : Node(num_input, trivial), inner_(unique_ptr<Node>(inner)) {}
        };

        class ExecutionGraph {
        protected:
            shared_ptr<Executor> executor_;
            vector<unique_ptr<Node>> nodes_;
            vector<unique_ptr<vector<Node *>>> downstream_;
            vector<unique_ptr<vector<Node *>>> upstream_;
            vector<unique_ptr<NodeOutput>> results_;

            vector<uint32_t> edge_weight_;

            vector<Node *> sources_;
            uint32_t num_dest_;
            vector<uint8_t> destinations_;

            bool concurrent_;
            Semaphore *done_;

            void init();

            void buildFlow();

            void executeNode(Node *);

            void executeNodeAsync(Node *);

            void executeNodeSync(Node *);

        public:
            uint32_t add(Node *, initializer_list<uint32_t>);

            void execute(bool concurrent = true);

            NodeOutput *result(uint32_t);
        };
    }
}


#endif //ARROW_PARALLEL_H
