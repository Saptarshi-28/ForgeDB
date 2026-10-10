#include "raft/RaftHeartbeat.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::raft;

    const std::string state_file =
        "build/raft_heartbeat_leader.meta";

    const std::string log_file =
        "build/raft_heartbeat_leader.log";

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
     * Start term 1 as candidate,
     * then simulate winning election.
     */
    uint64_t term =
        state.startElection();

    assert(term == 1);

    state.setRole(
        RaftRole::LEADER
    );

    assert(
        state.role() ==
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

    RaftHeartbeat heartbeat(
        "node1",
        state,
        log,
        peers
    );

    auto result =
        heartbeat.sendHeartbeats();

    std::cout
        << "\nHeartbeat result\n"
        << "Contacted: "
        << result.peers_contacted
        << "\nSuccessful: "
        << result.successful
        << "\nStepped down: "
        << (
            result.stepped_down
                ? "yes"
                : "no"
        )
        << "\n";

    assert(
        !result.stepped_down
    );

    assert(
        state.role() ==
        RaftRole::LEADER
    );

    std::cout
        << "Heartbeat round passed\n";

    return 0;
}