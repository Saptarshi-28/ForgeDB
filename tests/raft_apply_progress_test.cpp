#include "raft/RaftApplyProgress.h"
#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

int main()
{
    using namespace forgedb::raft;

    const std::string log_file =
        "build/raft_apply_progress_test.log";

    const std::string applied_file =
        "build/raft_apply_progress_test.applied";

    for (const auto& path : {
        log_file,
        log_file + ".tmp",
        log_file + ".commit",
        log_file + ".commit.tmp",
        applied_file,
        applied_file + ".tmp"
    }) {
        std::filesystem::remove(path);
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

    {
        RaftLog log(log_file);

        log.append(1, "SET a one");
        log.append(1, "SET b two");
        log.append(2, "DELETE a");

        // Only entries 1 and 2 committed.
        log.protectCommittedThrough(2);

        RaftApplyProgress progress(log, applied_file);

        assert(progress.lastApplied() == 0);

        // Cannot skip index 1.
        assert(rejected([&] {
            progress.recordApplied(2);
        }));

        // Cannot apply uncommitted index 3.
        assert(rejected([&] {
            progress.recordApplied(3);
        }));

        // Simulate successful durable KV applications.
        progress.recordApplied(1);
        progress.recordApplied(2);

        assert(progress.lastApplied() == 2);

        std::cout
            << "Ordered committed application passed\n";
    }

    // Reopen files to simulate a restart.
    {
        RaftLog log(log_file);
        RaftApplyProgress progress(log, applied_file);

        assert(log.protectedIndex() == 2);
        assert(progress.lastApplied() == 2);

        // Retrying the current checkpoint is harmless.
        progress.recordApplied(2);

        // Older progress must not overwrite it.
        assert(rejected([&] {
            progress.recordApplied(1);
        }));

        // Simulate a new committed entry.
        log.protectCommittedThrough(3);
        progress.recordApplied(3);

        assert(progress.lastApplied() == 3);

        std::cout
            << "Restart and advancement passed\n";
    }

    {
        RaftLog log(log_file);
        RaftApplyProgress progress(log, applied_file);

        assert(progress.lastApplied() == 3);

        std::cout
            << "Durable lastApplied recovery passed\n";
    }

    // Corrupt metadata must not be silently accepted.
    {
        std::ofstream file(
            applied_file,
            std::ios::trunc
        );

        file << "invalid\n";
    }

    {
        RaftLog log(log_file);

        assert(rejected([&] {
            RaftApplyProgress progress(log, applied_file);
        }));

        std::cout
            << "Corrupt checkpoint rejected\n";
    }

    std::cout
        << "All Raft application-progress tests passed\n";

    return 0;
}