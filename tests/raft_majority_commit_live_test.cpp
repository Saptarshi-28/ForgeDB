#include "raft/RaftHeartbeatLoop.h"
#include "raft/RaftLeaderProgress.h"
#include "raft/RaftCommitState.h"
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
        "build/raft_majority_live.meta";

    const std::string log_file =
        "build/raft_majority_live.log";

    std::filesystem::remove(state_file);
    std::filesystem::remove(state_file + ".tmp");
    std::filesystem::remove(log_file);
    std::filesystem::remove(log_file + ".tmp");
    std::filesystem::remove(log_file + ".commit");
    std::filesystem::remove(log_file + ".commit.tmp");

    RaftState state(
        "node1",
        state_file
    );

    RaftLog log(
        log_file
    );

    state.updateTerm(4000);
    state.setRole(RaftRole::LEADER);

    /*
     * Entry belongs to current term.
     */
    auto entry = log.append(
        4000,
        "SET majority yes"
    );

    assert(entry.index == 1);

    /*
     * Three-node configuration.
     * Only node2 will be online.
     */
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

    RaftLeaderProgress progress(peers);
    RaftCommitState commit(log);

    assert(commit.commitIndex() == 0);

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

    /*
     * Wait for automatic replication
     * and majority commit.
     */
    bool committed = false;

    for (int i = 0; i < 100; ++i) {

        if (commit.commitIndex() >= 1) {
            committed = true;
            break;
        }

        std::this_thread::sleep_for(100ms);
    }

    if (committed) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds{1000}
        );
    }

    loop.stop();

    assert(committed);

    assert(
        commit.commitIndex() == 1
    );

    assert(
        progress.matchIndex("node2") == 1
    );

    assert(
        progress.matchIndex("node3") == 0
    );

    std::cout
        << "Majority commit with one follower "
        << "offline passed\n";

    return 0;
}