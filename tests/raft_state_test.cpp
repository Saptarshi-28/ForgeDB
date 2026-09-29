#include "raft/RaftState.h"

#include <iostream>

int main()
{
    forgedb::raft::RaftState state(
        "node1",
        "raft_test.meta"
    );

    std::cout
        << "Node: "
        << state.nodeId()
        << "\n";

    std::cout
        << "Initial term: "
        << state.currentTerm()
        << "\n";

    state.updateTerm(1);

    bool vote =
        state.recordVote(
            "node2"
        );

    std::cout
        << "Term: "
        << state.currentTerm()
        << "\n";

    std::cout
        << "Vote recorded: "
        << (
            vote
                ? "yes"
                : "no"
        )
        << "\n";

    std::cout
        << "Voted for: "
        << state.votedFor()
        << "\n";

    return 0;
}