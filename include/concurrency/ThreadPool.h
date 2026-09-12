#pragma once
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <cstddef>
#include <functional>

namespace forgedb::concurrency {
    class ThreadPool {
        public:
            ThreadPool(size_t num_threads);
            ~ThreadPool();

            void submit(std::function<void()> job);
        
        private:
            std::vector<std::thread> workers_;
            std::queue<std::function<void()>> jobs_;
            
            std::mutex mutex_;
            std::condition_variable condition_;

            bool stopping_=false;
    };
}