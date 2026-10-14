#include "raft/RaftHeartbeatLoop.h"
#include "raft/RaftLeaderProgress.h"
#include "raft/RaftLog.h"
#include "raft/RaftState.h"
#include "raft/RaftCommitState.h"
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
        "build/raft_divergent_leader.meta";

    const std::string log_file =
        "build/raft_divergent_leader.log";

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
     * Leader's authoritative history.
     */
    log.append(
        1,
        "SET a one"
    );

    log.append(
        1,
        "SET b two"
    );

    log.append(
        3,
        "SET fresh three"
    );

    log.append(
        3,
        "SET fresh four"
    );

    log.append(
        3,
        "SET fresh five"
    );

    /*
     * RPC term is deliberately much newer
     * than anything node3 may generate
     * while starting up.
     */
    state.updateTerm(
        3000
    );

    state.setRole(
        RaftRole::LEADER
    );

    std::vector<RaftPeer> peers{
        {
            "node3",
            "127.0.0.1",
            6381
        }
    };

    RaftLeaderProgress progress(peers);
    RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

    RaftHeartbeatLoop loop(
        "node1",
        state,
        log,
        peers,
        progress,
        commit,
        200ms
    );

    loop.start();

    bool repaired = false;

    for (
        int i = 0;
        i < 100;
        ++i
    ) {
        if (
            progress.matchIndex(
                "node3"
            ) == 5
        ) {
            repaired = true;
            break;
        }

        std::this_thread::sleep_for(
            100ms
        );
    }

    assert(
        repaired
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

    loop.stop();

    std::cout
        << "Divergent follower automatically repaired\n";

    return 0;
}
