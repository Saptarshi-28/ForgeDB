#include "raft/RaftElectionLoop.h"

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
        "build/raft_election_loop.meta";

    const std::string log_file =
        "build/raft_election_loop.log";

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

    ElectionTimer timer(
        200ms,
        200ms
    );

    RaftElectionLoop loop(
        "node1",
        state,
        log,
        peers,
        timer,
        20ms
    );

    assert(
        state.role() ==
        RaftRole::FOLLOWER
    );

    loop.start();

    assert(
        loop.running()
    );

    /*
     * 200 ms timeout + network RPCs.
     * 800 ms gives plenty of room.
     */
    std::this_thread::sleep_for(
        800ms
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
        state.currentTerm() >= 1
    );

    std::cout
        << "Automatic election loop passed\n";

    return 0;
}