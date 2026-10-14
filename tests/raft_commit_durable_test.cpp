#include "raft/RaftCommitState.h"
#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

int main()
{
    using namespace forgedb::raft;

    const std::string file =
        "build/raft_commit_durable.log";

    for (const char* suffix : {
        "", ".tmp", ".commit", ".commit.tmp"
    }) {
        std::filesystem::remove(
            file + suffix
        );
    }

    /*
     * First process lifetime:
     * Commit index 2 through majority.
     */
    {
        RaftLog log(file);

        log.append(1, "SET a one");
        log.append(3, "SET b two");
        log.append(3, "SET c stale");

        RaftCommitState commit(log);

        assert(commit.commitIndex() == 0);

        // Leader + node2 have index 2.
        assert(
            commit.advanceLeaderCommit(
                3, {2, 0}, log
            ) == 2
        );

        assert(log.protectedIndex() == 2);

        std::cout
            << "Leader commit persisted\n";
    }

    /*
     * Restart: recover committed state.
     */
    {
        RaftLog log(file);
        RaftCommitState commit(log);

        assert(commit.commitIndex() == 2);
        assert(log.protectedIndex() == 2);

        bool rejected = false;

        try {
            log.truncateSuffix(2);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);

        /*
         * Replacement of uncommitted
         * index 3 is still allowed.
         */
        log.appendFromLeader({
            {3, 4, "SET c fresh"}
        });

        assert(
            log.entryAt(3)->term == 4
        );

        /*
         * Follower now learns that
         * index 3 is committed.
         */
        assert(
            commit.advanceFollowerCommit(
                3, 3
            ) == 3
        );

        assert(log.protectedIndex() == 3);

        std::cout
            << "Follower commit persisted\n";
    }

    /*
     * Second restart:
     * Both commitment and protection
     * must survive.
     */
    {
        RaftLog log(file);
        RaftCommitState commit(log);

        assert(commit.commitIndex() == 3);
        assert(log.protectedIndex() == 3);

        // Delayed older heartbeat.
        assert(
            commit.advanceFollowerCommit(
                1, 3
            ) == 3
        );

        bool rejected = false;

        try {
            log.truncateSuffix(3);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);

        std::cout
            << "Restart recovery and protection passed\n";
    }

    std::cout
        << "All durable commit integration tests passed\n";

    return 0;
}