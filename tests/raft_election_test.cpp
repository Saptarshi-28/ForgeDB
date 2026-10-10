#include "raft/RaftElection.h"

#include <cassert>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::raft;

    RaftState state(
        "node1",
        "build/raft_candidate.meta"
    );

    RaftLog log(
        "build/raft_candidate.log"
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

    RaftElection election(
        "node1",
        state,
        log,
        peers
    );

    ElectionResult result =
        election.runElection();

    std::cout
        << "\nElection result\n"
        << "Term: "
        << result.term
        << "\nVotes: "
        << result.votes_received
        << "/"
        << result.cluster_size
        << "\nWon: "
        << (
            result.won
                ? "yes"
                : "no"
        )
        << "\n";

    if (!result.won) {

        std::cerr
            << "Election failed\n";

        return 1;
    }

    assert(
        state.role() ==
        RaftRole::LEADER
    );

    std::cout
        << "Leader election passed\n";

    return 0;
}