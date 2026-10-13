#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <vector>

int main()
{
    using namespace forgedb::raft;

    const std::string filename =
        "build/raft_log_conflict_test.log";

    std::filesystem::remove(
        filename
    );

    std::filesystem::remove(
        filename + ".tmp"
    );

    /*
     * -----------------------------------
     * Test 1:
     * Build divergent follower log.
     * -----------------------------------
     */
    {
        RaftLog follower(
            filename
        );

        follower.append(
            1,
            "SET a one"
        );

        follower.append(
            1,
            "SET b two"
        );

        follower.append(
            2,
            "SET x old"
        );

        follower.append(
            2,
            "SET y old"
        );

        auto [
            index,
            term
        ] =
            follower.lastLogInfo();

        assert(
            index == 4
        );

        assert(
            term == 2
        );
    }

    /*
     * -----------------------------------
     * Test 2:
     * Leader replaces conflicting suffix.
     * -----------------------------------
     */
    {
        RaftLog follower(
            filename
        );

        std::vector<RaftLogEntry>
            leader_entries{
                {
                    3,
                    3,
                    "SET x new"
                },
                {
                    4,
                    3,
                    "SET y new"
                },
                {
                    5,
                    3,
                    "SET z five"
                }
            };

        follower.appendFromLeader(
            leader_entries
        );

        auto entries =
            follower.load();

        assert(
            entries.size() == 5
        );

        assert(
            entries[0].command ==
            "SET a one"
        );

        assert(
            entries[1].command ==
            "SET b two"
        );

        assert(
            entries[2].index == 3
        );

        assert(
            entries[2].term == 3
        );

        assert(
            entries[2].command ==
            "SET x new"
        );

        assert(
            entries[3].index == 4
        );

        assert(
            entries[3].term == 3
        );

        assert(
            entries[3].command ==
            "SET y new"
        );

        assert(
            entries[4].index == 5
        );

        assert(
            entries[4].term == 3
        );

        assert(
            entries[4].command ==
            "SET z five"
        );

        std::cout
            << "Conflict replacement passed\n";
    }

    /*
     * -----------------------------------
     * Test 3:
     * Repair survives restart.
     * -----------------------------------
     */
    {
        RaftLog restarted(
            filename
        );

        auto [
            index,
            term
        ] =
            restarted.lastLogInfo();

        assert(
            index == 5
        );

        assert(
            term == 3
        );

        std::cout
            << "Conflict repair restart passed\n";
    }

    /*
     * -----------------------------------
     * Test 4:
     * Duplicate leader entries must be
     * idempotent.
     * -----------------------------------
     */
    {
        RaftLog follower(
            filename
        );

        std::vector<RaftLogEntry>
            duplicate{
                {
                    3,
                    3,
                    "SET x new"
                },
                {
                    4,
                    3,
                    "SET y new"
                },
                {
                    5,
                    3,
                    "SET z five"
                }
            };

        follower.appendFromLeader(
            duplicate
        );

        auto entries =
            follower.load();

        assert(
            entries.size() == 5
        );

        std::cout
            << "Duplicate AppendEntries passed\n";
    }

    /*
     * -----------------------------------
     * Test 5:
     * Follower simply behind leader.
     * -----------------------------------
     */
    {
        RaftLog follower(
            filename
        );

        std::vector<RaftLogEntry>
            missing_entries{
                {
                    6,
                    4,
                    "SET six value"
                },
                {
                    7,
                    4,
                    "DELETE a"
                }
            };

        follower.appendFromLeader(
            missing_entries
        );

        auto [
            index,
            term
        ] =
            follower.lastLogInfo();

        assert(
            index == 7
        );

        assert(
            term == 4
        );

        std::cout
            << "Follower catch-up append passed\n";
    }

    /*
     * -----------------------------------
     * Test 6:
     * Reject a gap.
     * -----------------------------------
     */
    {
        RaftLog follower(
            filename
        );

        bool rejected = false;

        try {
            follower.appendFromLeader({
                {
                    10,
                    5,
                    "SET gap invalid"
                }
            });
        }
        catch (
            const std::runtime_error&
        ) {
            rejected = true;
        }

        assert(rejected);

        auto [
            index,
            term
        ] =
            follower.lastLogInfo();

        assert(
            index == 7
        );

        assert(
            term == 4
        );

        std::cout
            << "Gap rejection passed\n";
    }

    /*
     * -----------------------------------
     * Test 7:
     * Same index + same term but a
     * different command is corruption.
     * -----------------------------------
     */
    {
        RaftLog follower(
            filename
        );

        bool rejected = false;

        try {
            follower.appendFromLeader({
                {
                    7,
                    4,
                    "SET impossible conflict"
                }
            });
        }
        catch (
            const std::runtime_error&
        ) {
            rejected = true;
        }

        assert(rejected);

        std::cout
            << "Same-term conflict rejected\n";
    }

    std::cout
        << "All Raft conflict tests passed\n";

    return 0;
}