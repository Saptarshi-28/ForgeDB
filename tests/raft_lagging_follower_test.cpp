#include "raft/RaftHeartbeatLoop.h"
#include "raft/RaftLeaderProgress.h"
#include "raft/RaftLog.h"
#include "raft/RaftState.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

int main()
{
    using namespace forgedb::raft;
    using namespace std::chrono_literals;

    const std::string state_file =
        "build/raft_lagging_follower.meta";

    const std::string log_file =
        "build/raft_lagging_follower.log";

    std::filesystem::remove(
        state_file
    );

    std::filesystem::remove(
        state_file + ".tmp"
    );

    std::filesystem::remove(
        log_file
    );

    std::filesystem::remove(
        log_file + ".tmp"
    );

    RaftState state(
        "node1",
        state_file
    );

    RaftLog log(
        log_file
    );

    state.updateTerm(
        2000
    );

    state.setRole(
        RaftRole::LEADER
    );

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

    RaftHeartbeatLoop loop(
        "node1",
        state,
        log,
        peers,
        progress,
        200ms
    );

    loop.start();

    /*
     * Allow leader progress initialization.
     *
     * node2 should be online.
     * node3 should still be offline.
     */
    std::this_thread::sleep_for(
        500ms
    );

    /*
     * Create three leader entries.
     */
    log.append(
        2000,
        "SET one value"
    );

    log.append(
        2000,
        "SET two value"
    );

    log.append(
        2000,
        "SET three value"
    );

    /*
     * node2 should receive all three.
     *
     * node3 is deliberately offline.
     */
    bool node2_caught_up = false;

    for (
        int i = 0;
        i < 50;
        ++i
    ) {
        if (
            progress.matchIndex(
                "node2"
            ) >= 3
        ) {
            node2_caught_up = true;
            break;
        }

        std::this_thread::sleep_for(
            100ms
        );
    }

    assert(
        node2_caught_up
    );

    assert(
        progress.matchIndex(
            "node3"
        ) == 0
    );

    std::cout
        << "\nnode2 reached index 3\n";

    std::cout
        << "node3 is still behind\n\n";

    std::cout
        << "START NODE3 NOW\n"
        << std::flush;

    /*
     * Give us plenty of time to start
     * node3 in another terminal.
     */
    bool node3_caught_up = false;

    for (
        int i = 0;
        i < 300;
        ++i
    ) {
        if (
            progress.matchIndex(
                "node3"
            ) >= 3
        ) {
            node3_caught_up = true;
            break;
        }

        std::this_thread::sleep_for(
            100ms
        );
    }

    assert(
        node3_caught_up
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 4
    );

    std::cout
        << "node3 automatically caught up "
        << "through index 3\n";

    loop.stop();

    std::cout
        << "Lagging follower recovery passed\n";

    return 0;
}