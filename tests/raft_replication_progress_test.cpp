#include "raft/RaftHeartbeat.h"
#include "raft/RaftLeaderProgress.h"
#include "raft/RaftLog.h"
#include "raft/RaftState.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::raft;

    const std::string state_file =
        "build/raft_replication_progress.meta";

    const std::string log_file =
        "build/raft_replication_progress.log";

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

    /*
     * Leader has five entries.
     */
    log.append(
        1,
        "SET one value"
    );

    log.append(
        1,
        "SET two value"
    );

    log.append(
        2,
        "SET three value"
    );

    log.append(
        2,
        "SET four value"
    );

    log.append(
        3,
        "SET five value"
    );

    /*
     * Make node1 leader in a term newer
     * than our test follower.
     */
    state.startElection();

    state.updateTerm(
        10
    );

    state.setRole(
        RaftRole::LEADER
    );

    std::vector<RaftPeer> peers{
        {
            "node2",
            "127.0.0.1",
            6380
        }
    };

    RaftLeaderProgress progress(
        peers
    );

    /*
     * A newly elected leader optimistically
     * assumes node2 may already contain all
     * five entries.
     */
    progress.reset(
        5
    );

    assert(
        progress.nextIndex(
            "node2"
        ) == 6
    );

    RaftHeartbeat replication(
        "node1",
        state,
        log,
        peers,
        progress
    );

    /*
     * node2 should initially reject until
     * nextIndex reaches a matching prefix.
     *
     * Calling several rounds simulates
     * periodic leader replication.
     */
    for (
        int i = 0;
        i < 8;
        ++i
    ) {
        auto result =
            replication.sendHeartbeats();

        assert(
            !result.stepped_down
        );

        if (
            progress.matchIndex(
                "node2"
            ) == 5
        ) {
            break;
        }
    }

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
        << "Automatic follower backtracking "
        << "and catch-up passed\n";

    return 0;
}