
#include "raft/RaftState.h"
#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main()
{
    namespace fs = std::filesystem;
    using namespace forgedb::raft;

    const auto dir = fs::absolute(
        "build/raft_vote_log_snapshot_test_data"
    );

    fs::remove_all(dir);
    fs::create_directories(dir);
    fs::current_path(dir);

    RaftState state("node1", "raft.meta");
    RaftLog log("raft.log");

    state.updateTerm(2);
    log.append(2, "SET key value");

    // An outdated candidate must not win.
    RequestVoteRequest stale{
        3, "candidate", 0, 0
    };

    auto response =
        state.handleRequestVoteWithLog(stale, log);

    assert(!response.vote_granted);
    assert(state.currentTerm() == 3);

    std::cout << "Outdated candidate rejected\n";

    // Candidate now reports an up-to-date log.
    RequestVoteRequest fresh{
        3, "candidate", 1, 2
    };

    response =
        state.handleRequestVoteWithLog(fresh, log);

    assert(response.vote_granted);

    std::cout << "Up-to-date candidate accepted\n";

    // Log changes before another election.
    log.append(3, "RAFT_NOOP");

    RequestVoteRequest outdated_again{
        4, "other", 1, 2
    };

    response = state.handleRequestVoteWithLog(
        outdated_again, log
    );

    assert(!response.vote_granted);

    RequestVoteRequest latest{
        4, "other", 2, 3
    };

    response =
        state.handleRequestVoteWithLog(latest, log);

    assert(response.vote_granted);

    std::cout << "Updated log snapshot respected\n";
    std::cout << "All voting snapshot tests passed\n";
}
