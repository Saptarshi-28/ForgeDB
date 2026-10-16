
#include "raft/RaftState.h"
#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main()
{
    namespace fs = std::filesystem;
    using namespace forgedb::raft;

    const fs::path dir =
        "build/raft_noop_election_test_data";

    fs::remove_all(dir);
    fs::create_directories(dir);

    RaftState state(
        "node1",
        (dir / "raft.meta").string()
    );

    RaftLog log(
        (dir / "raft.log").string()
    );

    // Simulate an inherited uncommitted entry.
    state.updateTerm(3);
    log.append(3, "SET inherited value");

    // Begin election in term 4.
    const uint64_t term = state.startElection();

    assert(term == 4);
    assert(state.role() == RaftRole::CANDIDATE);

    // Promote only after durably appending no-op.
    assert(
        state.becomeLeaderWithNoopIfCandidate(
            term, log
        )
    );

    assert(state.role() == RaftRole::LEADER);

    auto noop = log.entryAt(2);

    assert(noop.has_value());
    assert(noop->term == 4);
    assert(noop->command == "RAFT_NOOP");

    std::cout << "Leader no-op append passed\n";

    // A repeated promotion must not add another no-op.
    assert(
        !state.becomeLeaderWithNoopIfCandidate(
            term, log
        )
    );

    assert(log.lastLogInfo().first == 2);

    std::cout << "Duplicate no-op prevented\n";

    // Discover a higher term.
    state.updateTerm(5);

    assert(state.role() == RaftRole::FOLLOWER);

    // A stale election cannot regain leadership.
    assert(
        !state.becomeLeaderWithNoopIfCandidate(
            4, log
        )
    );

    assert(log.lastLogInfo().first == 2);

    std::cout << "Stale election rejected\n";

    // Start a fresh election.
    const uint64_t next_term =
        state.startElection();

    assert(next_term == 6);

    assert(
        state.becomeLeaderWithNoopIfCandidate(
            next_term, log
        )
    );

    auto next_noop = log.entryAt(3);

    assert(next_noop.has_value());
    assert(next_noop->term == 6);
    assert(next_noop->command == "RAFT_NOOP");

    // No majority commitment has happened.
    assert(log.protectedIndex() == 0);

    std::cout << "New-term no-op passed\n";
    std::cout << "All Raft election no-op tests passed\n";

    return 0;
}
