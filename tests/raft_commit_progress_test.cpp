#include "raft/RaftCommitState.h"
#include "raft/RaftLeaderProgress.h"
#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::raft;

    const std::string filename =
        "build/raft_commit_progress.log";

    std::filesystem::remove(filename);
    std::filesystem::remove(filename + ".tmp");

    RaftLog log(filename);

    /*
     * Leader's log:
     *
     * index 1, term 1
     * index 2, term 2
     */
    log.append(
        1,
        "SET old one"
    );

    log.append(
        2,
        "SET fresh two"
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

    progress.reset(2);

    RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

    /*
     * Simulate majority calculation
     * for a leader in term 2.
     */
    auto calculateCommit = [&]() {
        return commit.advanceLeaderCommit(
            2,
            progress.followerMatchIndexes(),
            log
        );
    };

    /*
     * Test 1:
     * Leader alone has both entries.
     *
     * node2 = 0
     * node3 = 0
     *
     * No majority.
     */
    assert(
        calculateCommit() == 0
    );

    std::cout
        << "Leader-alone commit rejected\n";

    /*
     * Test 2:
     * node2 has only old-term entry 1.
     *
     * leader = 2
     * node2  = 1
     * node3  = 0
     *
     * Majority candidate = 1.
     *
     * Entry 1 is from term 1,
     * so term 2 leader cannot
     * commit it by counting replicas.
     */
    progress.recordSuccess(
        "node2",
        1
    );

    assert(
        calculateCommit() == 0
    );

    std::cout
        << "Old-term candidate rejected\n";

    /*
     * Test 3:
     * node2 catches up to index 2.
     *
     * leader = 2
     * node2  = 2
     * node3  = 0
     *
     * 2 of 3 nodes have index 2.
     */
    progress.recordSuccess(
        "node2",
        2
    );

    assert(
        calculateCommit() == 2
    );

    assert(
        commit.commitIndex() == 2
    );

    std::cout
        << "Real progress majority passed\n";

    /*
     * Test 4:
     * node3 also catches up.
     */
    progress.recordSuccess(
        "node3",
        2
    );

    assert(
        calculateCommit() == 2
    );

    std::cout
        << "All followers caught up\n";

    /*
     * Test 5:
     * Reset volatile leader progress.
     *
     * Already committed entries
     * must remain committed.
     */
    progress.reset(2);

    assert(
        progress.matchIndex("node2") == 0
    );

    assert(
        progress.matchIndex("node3") == 0
    );

    assert(
        calculateCommit() == 2
    );

    std::cout
        << "Commit monotonicity preserved\n";

    std::cout
        << "All Raft commit progress tests passed\n";

    return 0;
}