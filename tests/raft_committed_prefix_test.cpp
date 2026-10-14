#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>

int main()
{
    using namespace forgedb::raft;

    const std::string filename =
        "build/raft_protected_prefix.log";

    for (
        const auto* suffix :
        {"", ".tmp", ".commit", ".commit.tmp"}
    ) {
        std::filesystem::remove(
            filename + suffix
        );
    }

    auto rejected = [](auto action) {
        try {
            action();
        }
        catch (const std::runtime_error&) {
            return true;
        }

        return false;
    };

    /*
     * First process lifetime.
     */
    {
        RaftLog log(filename);

        log.append(1, "SET a one");
        log.append(1, "SET b two");
        log.append(2, "SET c stale");
        log.append(2, "SET d stale");

        log.protectCommittedThrough(2);

        assert(log.protectedIndex() == 2);

        /*
         * Cannot truncate committed index 2.
         */
        assert(rejected([&] {
            log.truncateSuffix(2);
        }));

        /*
         * Cannot overwrite committed index 2.
         */
        assert(rejected([&] {
            log.appendFromLeader({
                {2, 3, "SET b forbidden"}
            });
        }));

        /*
         * Uncommitted suffix repair
         * must continue working.
         */
        log.appendFromLeader({
            {3, 3, "SET c fresh"},
            {4, 3, "SET d fresh"}
        });

        assert(log.entryAt(3)->term == 3);
        assert(log.protectedIndex() == 2);

        std::cout
            << "Committed prefix protected\n";
    }

    /*
     * Simulate process restart.
     */
    {
        RaftLog log(filename);

        assert(log.protectedIndex() == 2);

        assert(rejected([&] {
            log.truncateSuffix(1);
        }));

        /*
         * Truncating an uncommitted
         * entry is still allowed.
         */
        log.truncateSuffix(4);

        assert(
            log.lastLogInfo().first == 3
        );

        /*
         * Commit one more entry.
         */
        log.protectCommittedThrough(3);

        std::cout
            << "Restart recovery passed\n";
    }

    /*
     * Simulate another restart.
     */
    {
        RaftLog log(filename);

        assert(log.protectedIndex() == 3);

        assert(rejected([&] {
            log.truncateSuffix(3);
        }));

        assert(rejected([&] {
            log.appendFromLeader({
                {3, 4, "SET c forbidden"}
            });
        }));

        std::cout
            << "Durable protection passed\n";
    }

    std::cout
        << "All durable committed-prefix tests passed\n";

    return 0;
}