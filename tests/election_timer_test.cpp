#include "raft/ElectionTimer.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

int main()
{
    using namespace forgedb::raft;
    using namespace std::chrono_literals;

    /*
     * Fixed range makes this test
     * deterministic.
     */
    ElectionTimer timer(
        120ms,
        120ms
    );

    assert(
        !timer.expired()
    );

    auto first_remaining =
        timer.remaining();

    assert(
        first_remaining.count() > 0
    );

    std::cout
        << "Fresh timer passed\n";

    /*
     * Wait beyond the deadline.
     */
    std::this_thread::sleep_for(
        160ms
    );

    assert(
        timer.expired()
    );

    assert(
        timer.remaining().count() == 0
    );

    std::cout
        << "Expiration passed\n";

    /*
     * Reset should create a fresh
     * deadline.
     */
    timer.reset();

    assert(
        !timer.expired()
    );

    assert(
        timer.remaining().count() > 0
    );

    std::cout
        << "Reset passed\n";

    /*
     * Confirm that resetting actually
     * postpones expiration.
     */
    std::this_thread::sleep_for(
        70ms
    );

    assert(
        !timer.expired()
    );

    timer.reset();

    std::this_thread::sleep_for(
        70ms
    );

    assert(
        !timer.expired()
    );

    std::cout
        << "Deadline extension passed\n";

    std::cout
        << "All election timer tests passed\n";

    return 0;
}