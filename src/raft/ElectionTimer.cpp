#include "raft/ElectionTimer.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>

namespace forgedb::raft {

ElectionTimer::ElectionTimer(
    std::chrono::milliseconds minimum_timeout,
    std::chrono::milliseconds maximum_timeout
)
    : minimum_timeout_(minimum_timeout),
      maximum_timeout_(maximum_timeout),
      generator_(std::random_device{}())
{
    if (
        minimum_timeout_.count() <= 0 ||
        maximum_timeout_.count() <= 0 ||
        minimum_timeout_ >
            maximum_timeout_
    ) {
        throw std::invalid_argument(
            "Invalid Raft election timeout range"
        );
    }

    reset();
}

void ElectionTimer::reset()
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    std::uniform_int_distribution<
        int64_t
    > distribution(
        minimum_timeout_.count(),
        maximum_timeout_.count()
    );

    auto timeout =
        std::chrono::milliseconds(
            distribution(generator_)
        );

    deadline_ =
        Clock::now() + timeout;
}

bool ElectionTimer::expired() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return Clock::now() >= deadline_;
}

std::chrono::milliseconds
ElectionTimer::remaining() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    auto now =
        Clock::now();

    if (now >= deadline_) {
        return std::chrono::milliseconds{
            0
        };
    }

    return std::chrono::duration_cast<
        std::chrono::milliseconds
    >(
        deadline_ - now
    );
}

}