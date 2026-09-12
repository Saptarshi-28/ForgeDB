#include "concurrency/ThreadPool.h"

namespace forgedb::concurrency {

    ThreadPool::ThreadPool(size_t num_threads)
    {
        for (size_t i = 0; i < num_threads; ++i) {

            workers_.emplace_back([this]() {

                while (true) {

                    std::function<void()> job;

                    {
                        std::unique_lock<std::mutex> lock(mutex_);

                        condition_.wait(lock, [this]() {
                            return !jobs_.empty() || stopping_;
                        });

                        if (stopping_ && jobs_.empty()) {
                            return;
                        }

                        job = std::move(jobs_.front());
                        jobs_.pop();
                    }

                    job();
                }
            });
        }
    }
    void ThreadPool::submit(std::function<void()> job)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            jobs_.push(std::move(job));
        }
        condition_.notify_one();
    }

    ThreadPool::~ThreadPool()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_=true;
        }
        condition_.notify_all();
        
        for(auto& worker:workers_){
            if(worker.joinable()){
                worker.join();
            }
        }
    }

}