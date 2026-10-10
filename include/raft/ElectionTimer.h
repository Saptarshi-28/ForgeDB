#pragma once

#include <chrono>
#include <mutex>
#include <random>

namespace forgedb::raft {

class ElectionTimer {
public:
    ElectionTimer(
        std::chrono::milliseconds minimum_timeout,
        std::chrono::milliseconds maximum_timeout
    );

    void reset();

    bool expired() const;

    std::chrono::milliseconds
    remaining() const;

private:
    using Clock =
        std::chrono::steady_clock;

    std::chrono::milliseconds
        minimum_timeout_;

    std::chrono::milliseconds
        maximum_timeout_;

    mutable std::mutex mutex_;

    Clock::time_point deadline_;

    std::mt19937 generator_;
};

}