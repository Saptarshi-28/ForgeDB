#include "raft/RaftLeaderProgress.h"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include <vector>

int main()
{
    using namespace forgedb::raft;

    std::vector<RaftPeer> peers{
        {
            "node2",
            "127.0.0.1",
            6380
        },
        {
            "node3",
            "127.0.0.1",
            6381
        }
    };

    RaftLeaderProgress progress(
        peers
    );

    /*
     * New leader has log entries 1..5.
     */
    progress.reset(
        5
    );

    assert(
        progress.nextIndex(
            "node2"
        ) == 6
    );

    assert(
        progress.matchIndex(
            "node2"
        ) == 0
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 6
    );

    /*
     * node2 is fully up to date.
     */
    progress.recordSuccess(
        "node2",
        5
    );

    assert(
        progress.matchIndex(
            "node2"
        ) == 5
    );

    assert(
        progress.nextIndex(
            "node2"
        ) == 6
    );

    std::cout
        << "Success tracking passed\n";

    /*
     * node3 rejects our log prefix.
     *
     * Backtrack:
     *
     * 6 -> 5 -> 4 -> 3
     */
    progress.recordFailure(
        "node3"
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 5
    );

    progress.recordFailure(
        "node3"
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 4
    );

    progress.recordFailure(
        "node3"
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 3
    );

    assert(
        progress.matchIndex(
            "node3"
        ) == 0
    );

    std::cout
        << "Failure backtracking passed\n";

    /*
     * Leader successfully replicates
     * through index 5.
     */
    progress.recordSuccess(
        "node3",
        5
    );

    assert(
        progress.matchIndex(
            "node3"
        ) == 5
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 6
    );

    std::cout
        << "Follower catch-up tracking passed\n";

    /*
     * Stale acknowledgement must not
     * move progress backwards.
     */
    progress.recordSuccess(
        "node3",
        3
    );

    assert(
        progress.matchIndex(
            "node3"
        ) == 5
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 6
    );

    std::cout
        << "Stale acknowledgement ignored\n";

    /*
     * nextIndex must never go below 1.
     */
    progress.reset(
        0
    );

    assert(
        progress.nextIndex(
            "node2"
        ) == 1
    );

    progress.recordFailure(
        "node2"
    );

    progress.recordFailure(
        "node2"
    );

    assert(
        progress.nextIndex(
            "node2"
        ) == 1
    );

    std::cout
        << "Minimum nextIndex passed\n";

    /*
     * Unknown peers must be rejected.
     */
    bool rejected = false;

    try {
        progress.nextIndex(
            "node99"
        );
    }
    catch (
        const std::out_of_range&
    ) {
        rejected = true;
    }

    assert(rejected);

    std::cout
        << "Unknown peer rejection passed\n";

    std::cout
        << "All Raft leader progress tests passed\n";

    return 0;
}