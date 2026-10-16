
#include "raft/RaftLog.h"
#include "raft/RaftCommitState.h"
#include "raft/RaftApplyProgress.h"
#include "raft/RaftCommittedApplier.h"
#include "storage/KeyValueStore.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>

int main()
{
    namespace fs = std::filesystem;
    using namespace forgedb::raft;
    using forgedb::storage::KeyValueStore;

    const fs::path dir =
        "build/raft_noop_applier_test_data";

    // Use only a disposable test directory.
    fs::remove_all(dir);
    fs::create_directories(dir);
    fs::current_path(dir);

    // Test 1: committed SET followed by no-op.
    {
        RaftLog log("raft.log");
        RaftCommitState commit(log);
        RaftApplyProgress progress(log, "raft.applied");
        KeyValueStore store("forge.wal");

        RaftCommittedApplier applier(
            log, commit, progress, store
        );

        log.append(7, "SET key value");
        log.append(7, "RAFT_NOOP");

        // Neither entry is committed yet.
        assert(applier.applyCommitted() == 0);
        assert(progress.lastApplied() == 0);

        commit.advanceFollowerCommit(2, 2);

        // Both entries become applied.
        assert(applier.applyCommitted() == 2);
        assert(progress.lastApplied() == 2);

        // The SET still works.
        assert(store.get("key") == "value");

        std::cout << "Committed no-op passed\n";
    }

    // Test 2: recovery and malformed no-op.
    {
        RaftLog log("raft.log");
        RaftCommitState commit(log);
        RaftApplyProgress progress(log, "raft.applied");
        KeyValueStore store("forge.wal");

        RaftCommittedApplier applier(
            log, commit, progress, store
        );

        assert(commit.commitIndex() == 2);
        assert(progress.lastApplied() == 2);
        assert(store.get("key") == "value");

        // Nothing should be applied twice.
        assert(applier.applyCommitted() == 0);

        std::cout << "No-op restart recovery passed\n";

        log.append(7, "RAFT_NOOP extra");
        commit.advanceFollowerCommit(3, 3);

        bool rejected = false;

        try {
            applier.applyCommitted();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);
        assert(progress.lastApplied() == 2);

        std::cout << "Malformed no-op rejected\n";
    }

    std::cout
        << "All Raft no-op tests passed\n";

    return 0;
}
