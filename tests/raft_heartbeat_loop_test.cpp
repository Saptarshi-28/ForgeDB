#include "raft/RaftHeartbeatLoop.h"

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
        "build/raft_heartbeat_loop.meta";

    const std::string log_file =
        "build/raft_heartbeat_loop.log";

    std::filesystem::remove(
        state_file
    );

    std::filesystem::remove(
        state_file + ".tmp"
    );

    std::filesystem::remove(
        log_file
    );

    RaftState state(
        "node1",
        state_file
    );

    RaftLog log(
        log_file
    );

    /*
     * Simulate node1 winning
     * election term 1.
     */
    assert(
        state.startElection() == 1
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

    /*
     * Use 200 ms here to make the
     * test visibly quick.
     */
    RaftHeartbeatLoop loop(
        "node1",
        state,
        log,
        peers,
        200ms
    );

    loop.start();

    assert(
        loop.running()
    );

    /*
     * Enough time for several
     * heartbeat rounds.
     */
    std::this_thread::sleep_for(
        900ms
    );

    loop.stop();

    assert(
        !loop.running()
    );

    assert(
        state.role() ==
        RaftRole::LEADER
    );

    assert(
        state.currentTerm() == 1
    );

    std::cout
        << "Periodic heartbeat loop passed\n";

    return 0;
}