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
        "build/raft_live_replication.meta";

    const std::string log_file =
        "build/raft_live_replication.log";

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
     * Use a deliberately high term so
     * node2/node3 will recognize this
     * test leader.
     */
    state.updateTerm(
        1000
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

    /*
     * Allow the loop to initialize
     * nextIndex for leader term 1000.
     */
    std::this_thread::sleep_for(
        500ms
    );

    /*
     * Create a real leader log entry.
     *
     * We are NOT applying it to the KV
     * store yet.
     */
    auto first =
        log.append(
            1000,
            "SET alpha one"
        );

    assert(
        first.index == 1
    );

    /*
     * Wait until both followers have
     * acknowledged index 1.
     */
    bool first_replicated = false;

    for (
        int i = 0;
        i < 50;
        ++i
    ) {
        if (
            progress.matchIndex(
                "node2"
            ) >= 1 &&
            progress.matchIndex(
                "node3"
            ) >= 1
        ) {
            first_replicated = true;
            break;
        }

        std::this_thread::sleep_for(
            100ms
        );
    }

    assert(
        first_replicated
    );

    std::cout
        << "First live entry replicated\n";

    /*
     * Append another entry AFTER the
     * heartbeat loop is already running.
     *
     * This proves the loop notices newly
     * created log entries automatically.
     */
    auto second =
        log.append(
            1000,
            "SET beta two"
        );

    assert(
        second.index == 2
    );

    bool second_replicated = false;

    for (
        int i = 0;
        i < 50;
        ++i
    ) {
        if (
            progress.matchIndex(
                "node2"
            ) >= 2 &&
            progress.matchIndex(
                "node3"
            ) >= 2
        ) {
            second_replicated = true;
            break;
        }

        std::this_thread::sleep_for(
            100ms
        );
    }

    assert(
        second_replicated
    );

    std::cout
        << "Second live entry replicated\n";

    assert(
        progress.nextIndex(
            "node2"
        ) == 3
    );

    assert(
        progress.nextIndex(
            "node3"
        ) == 3
    );

    loop.stop();

    assert(
        !loop.running()
    );

    std::cout
        << "Automatic live Raft replication passed\n";

    return 0;
}