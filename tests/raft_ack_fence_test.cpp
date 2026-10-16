
#include "raft/RaftState.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main()
{
    namespace fs = std::filesystem;
    using namespace forgedb::raft;

    const fs::path dir =
        "build/raft_ack_fence_test_data";

    fs::remove_all(dir);
    fs::create_directories(dir);

    RaftState state(
        "node1",
        (dir / "raft.meta").string()
    );

    // Initially a follower.
    state.updateTerm(7);
    assert(!state.isLeaderInTerm(7));

    // A legitimate leader in term 7.
    state.setRole(RaftRole::LEADER);

    assert(state.isLeaderInTerm(7));
    assert(!state.isLeaderInTerm(6));
    assert(!state.isLeaderInTerm(8));

    std::cout << "Leader-term match passed\n";

    // A newer leader is discovered.
    state.observeLeader(8);

    assert(!state.isLeaderInTerm(7));
    assert(!state.isLeaderInTerm(8));

    std::cout << "Leadership change rejected\n";

    // A new election begins.
    const auto term = state.startElection();

    assert(term == 9);
    assert(!state.isLeaderInTerm(9));

    // Candidate becomes leader.
    state.setRole(RaftRole::LEADER);

    assert(state.isLeaderInTerm(9));
    assert(!state.isLeaderInTerm(7));

    std::cout << "New leader-term match passed\n";

    // Step down in the same term.
    state.setRole(RaftRole::FOLLOWER);

    assert(!state.isLeaderInTerm(9));

    std::cout << "Same-term stepdown rejected\n";
    std::cout << "All acknowledgment fencing tests passed\n";

    return 0;
}
