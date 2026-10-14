#include "raft/RaftCommitState.h"
#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    const std::string filename =
        "build/raft_commit_state.log";

    std::filesystem::remove(
        filename
    );

    std::filesystem::remove(
        filename + ".tmp"
    );

    RaftLog log(
        filename
    );

    /*
     * Log:
     *
     * 1 -> term 1
     * 2 -> term 1
     * 3 -> term 2
     * 4 -> term 3
     * 5 -> term 3
     */
    log.append(
        1,
        "SET a one"
    );

    log.append(
        1,
        "SET b two"
    );

    log.append(
        2,
        "SET c three"
    );

    log.append(
        3,
        "SET d four"
    );

    log.append(
        3,
        "SET e five"
    );

    /*
     * Old-term majority candidate.
     *
     * [leader=5, node2=2, node3=2]
     *
     * Candidate = 2, but index 2
     * belongs to term 1.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        auto result =
            commit.advanceLeaderCommit(
                3,
                {
                    2,
                    2
                },
                log
            );

        assert(
            result == 0
        );

        std::cout
            << "Old-term majority rejected\n";
    }

    /*
     * [5,4,2]
     *
     * index 4 exists on a majority and
     * belongs to current term 3.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        auto result =
            commit.advanceLeaderCommit(
                3,
                {
                    4,
                    2
                },
                log
            );

        assert(
            result == 4
        );

        assert(
            commit.commitIndex() == 4
        );

        std::cout
            << "Commit to index 4 passed\n";
    }

    /*
     * Advance 4 -> 5.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        assert(
            commit.advanceLeaderCommit(
                3,
                {
                    4,
                    2
                },
                log
            ) == 4
        );

        assert(
            commit.advanceLeaderCommit(
                3,
                {
                    5,
                    2
                },
                log
            ) == 5
        );

        std::cout
            << "Commit advancement passed\n";
    }

    /*
     * commitIndex must never go backwards.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        assert(
            commit.advanceLeaderCommit(
                3,
                {
                    5,
                    5
                },
                log
            ) == 5
        );

        assert(
            commit.advanceLeaderCommit(
                3,
                {
                    2,
                    2
                },
                log
            ) == 5
        );

        std::cout
            << "Commit monotonicity passed\n";
    }

    /*
     * One follower may be unavailable.
     *
     * Leader + node2 still form a
     * majority in a three-node cluster.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        auto result =
            commit.advanceLeaderCommit(
                3,
                {
                    5,
                    0
                },
                log
            );

        assert(
            result == 5
        );

        std::cout
            << "One follower down passed\n";
    }

    /*
     * Leader alone cannot commit.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        auto result =
            commit.advanceLeaderCommit(
                3,
                {
                    0,
                    0
                },
                log
            );

        assert(
            result == 0
        );

        std::cout
            << "Leader-alone commit rejected\n";
    }
    /*
     * Test 7:
     * Follower cannot commit entries it
     * has not received.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        /*
         * Leader has committed through 5,
         * but follower only has entries 1,2.
         */
        auto result =
            commit.advanceFollowerCommit(
                5,
                2
            );

        assert(result == 2);

        assert(
            commit.commitIndex() == 2
        );

        std::cout
            << "Follower commit bounded by log passed\n";
    }

    /*
     * Test 8:
     * Received does not mean committed.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        /*
         * Follower has entries 1..5,
         * but leader has committed only 2.
         */
        auto result =
            commit.advanceFollowerCommit(
                2,
                5
            );

        assert(result == 2);

        std::cout
            << "Uncommitted follower entries preserved\n";
    }

    /*
     * Test 9:
     * Follower commit index cannot decrease.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        assert(
            commit.advanceFollowerCommit(
                4,
                5
            ) == 4
        );

        /*
         * Old AppendEntries carries
         * leaderCommit = 2.
         */
        assert(
            commit.advanceFollowerCommit(
                2,
                5
            ) == 4
        );

        assert(
            commit.commitIndex() == 4
        );

        std::cout
            << "Follower commit monotonicity passed\n";
    }

    /*
     * Test 10:
     * Follower learns about newly
     * committed entries.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        assert(
            commit.advanceFollowerCommit(
                2,
                5
            ) == 2
        );

        assert(
            commit.advanceFollowerCommit(
                5,
                5
            ) == 5
        );

        std::cout
            << "Follower commit advancement passed\n";
    }

    /*
     * Test 11:
     * Detect loss of committed entries.
     */
    {
        RaftCommitState commit(
    RaftCommitState::VolatileForTests{}
);

        commit.advanceFollowerCommit(
            5,
            5
        );

        bool rejected = false;

        try {
            commit.advanceFollowerCommit(
                6,
                4
            );
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);

        assert(
            commit.commitIndex() == 5
        );

        std::cout
            << "Committed log loss detected\n";
    }

    std::cout
        << "All Raft commit-state tests passed\n";

    return 0;
}