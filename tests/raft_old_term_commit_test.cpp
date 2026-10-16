
#include "raft/RaftLog.h"
#include "raft/RaftCommitState.h"
#include "raft/RaftApplyProgress.h"
#include "raft/RaftCommittedApplier.h"
#include "storage/KeyValueStore.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main()
{
    namespace fs = std::filesystem;
    using namespace forgedb::raft;
    using forgedb::storage::KeyValueStore;

    const fs::path dir = fs::absolute(
        "build/raft_old_term_commit_test_data"
    );

    // Isolated disposable test directory.
    fs::remove_all(dir);
    fs::create_directories(dir);
    fs::current_path(dir);

    {
        RaftLog log("raft.log");
        RaftCommitState commit(log);
        RaftApplyProgress progress(
            log, "raft.applied"
        );

        KeyValueStore store("forge.wal");

        RaftCommittedApplier applier(
            log, commit, progress, store
        );

        // --------------------------------
        // TEST 1: Inherited old-term entry
        // --------------------------------

        auto old_entry = log.append(
            7, "SET inherited durable"
        );

        assert(old_entry.index == 1);
        assert(old_entry.term == 7);

        // Three-node cluster:
        // Leader index = 1
        // Followers = {1, 0}
        //
        // A majority has index 1.
        // But the new leader is in term 8.

        assert(
            commit.advanceLeaderCommit(
                8, {1, 0}, log
            ) == 0
        );

        assert(commit.commitIndex() == 0);
        assert(applier.applyCommitted() == 0);
        assert(progress.lastApplied() == 0);

        std::cout
            << "Old-term direct commit rejected\n";

        // --------------------------------
        // TEST 2: Append current-term NOOP
        // --------------------------------

        auto noop = log.append(
            8, "RAFT_NOOP"
        );

        assert(noop.index == 2);
        assert(noop.term == 8);

        // The no-op exists on the leader,
        // but followers have not yet
        // replicated index 2.

        assert(
            commit.advanceLeaderCommit(
                8, {1, 0}, log
            ) == 0
        );

        assert(commit.commitIndex() == 0);
        assert(progress.lastApplied() == 0);

        std::cout
            << "Unreplicated no-op not committed\n";

        // --------------------------------
        // TEST 3: Current-term majority
        // --------------------------------

        // Simulate verified match indexes:
        //
        // Leader     = 2
        // Follower 1 = 2
        // Follower 2 = 0
        //
        // Majority now contains index 2.

        assert(
            commit.advanceLeaderCommit(
                8, {2, 0}, log
            ) == 2
        );

        assert(commit.commitIndex() == 2);
        assert(log.protectedIndex() == 2);

        std::cout
            << "Current-term no-op committed\n";

        // --------------------------------
        // TEST 4: Apply inherited entry
        // --------------------------------

        // The applier processes:
        // 1. SET inherited durable
        // 2. RAFT_NOOP

        assert(applier.applyCommitted() == 2);

        assert(progress.lastApplied() == 2);

        assert(
            store.get("inherited") == "durable"
        );

        // Nothing should apply again.
        assert(applier.applyCommitted() == 0);

        std::cout
            << "Inherited SET applied through no-op\n";
    }

    // ------------------------------------
    // TEST 5: Durable restart recovery
    // ------------------------------------

    {
        RaftLog log("raft.log");
        RaftCommitState commit(log);

        RaftApplyProgress progress(
            log, "raft.applied"
        );

        KeyValueStore store("forge.wal");

        RaftCommittedApplier applier(
            log, commit, progress, store
        );

        assert(commit.commitIndex() == 2);
        assert(log.protectedIndex() == 2);
        assert(progress.lastApplied() == 2);

        assert(
            store.get("inherited") == "durable"
        );

        assert(applier.applyCommitted() == 0);

        std::cout
            << "Old-term commit recovery passed\n";
    }

    std::cout
        << "All old-term Raft safety tests passed\n";

    return 0;
}
