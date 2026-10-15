#include "raft/RaftCommittedApplier.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

int main()
{
    using namespace forgedb::raft;
    using forgedb::storage::KeyValueStore;

    /*
     * Isolate the test because KeyValueStore
     * creates SSTables relative to the
     * process's working directory.
     */
    const fs::path test_directory =
        "build/raft_committed_applier_test_data";

    fs::remove_all(test_directory);
    fs::create_directories(test_directory);
    fs::current_path(test_directory);

    /*
     * First process lifetime.
     */
    {
        RaftLog log("raft.log");

        log.append(1, "SET a one");
        log.append(1, "SET b two");
        log.append(1, "DELETE a");

        RaftCommitState commit(log);

        RaftApplyProgress progress(
            log,
            "raft.applied"
        );

        KeyValueStore store("kv.wal");

        RaftCommittedApplier applier(
            log,
            commit,
            progress,
            store
        );

        /*
         * Nothing committed yet.
         */
        assert(applier.applyCommitted() == 0);

        assert(store.get("a").empty());

        assert(progress.lastApplied() == 0);

        std::cout
            << "Uncommitted entries not applied\n";

        /*
         * Commit only index 1.
         */
        commit.advanceFollowerCommit(
            1,
            3
        );

        assert(applier.applyCommitted() == 1);

        assert(store.get("a") == "one");

        /*
         * Index 2 is still uncommitted.
         */
        assert(store.get("b").empty());

        assert(progress.lastApplied() == 1);

        std::cout
            << "Only committed entry applied\n";

        /*
         * Commit entries 2 and 3.
         */
        commit.advanceFollowerCommit(
            3,
            3
        );

        assert(applier.applyCommitted() == 2);

        assert(store.get("b") == "two");

        assert(store.get("a").empty());

        assert(progress.lastApplied() == 3);

        std::cout
            << "Ordered SET and DELETE passed\n";

        /*
         * DELETE of missing key still
         * counts as an applied operation.
         */
        log.append(
            1,
            "DELETE missing"
        );

        commit.advanceFollowerCommit(
            4,
            4
        );

        assert(applier.applyCommitted() == 1);

        assert(progress.lastApplied() == 4);

        std::cout
            << "Missing-key DELETE passed\n";

        /*
         * Simulate a crash window:
         *
         * Index 5 is committed.
         * Its KV operation becomes durable.
         * The process crashes BEFORE
         * lastApplied advances.
         */
        log.append(
            1,
            "SET b updated"
        );

        commit.advanceFollowerCommit(
            5,
            5
        );

        /*
         * Simulate the applier having
         * completed its KV write, but not
         * its progress checkpoint.
         */
        store.set(
            "b",
            "updated"
        );

        assert(store.get("b") == "updated");

        assert(progress.lastApplied() == 4);
    }

    /*
     * Restart simulation:
     *
     * Reconstruct all persistent components.
     */
    {
        RaftLog log("raft.log");
        RaftCommitState commit(log);

        RaftApplyProgress progress(
            log,
            "raft.applied"
        );

        KeyValueStore store("kv.wal");

        assert(commit.commitIndex() == 5);

        assert(progress.lastApplied() == 4);

        /*
         * The KV WAL recovers the value
         * written before the simulated crash.
         */
        assert(store.get("b") == "updated");

        RaftCommittedApplier applier(
            log,
            commit,
            progress,
            store
        );

        /*
         * Reapply index 5.
         *
         * SET is idempotent, so replaying
         * the same operation is safe.
         */
        assert(applier.applyCommitted() == 1);

        assert(progress.lastApplied() == 5);

        assert(store.get("b") == "updated");

        /*
         * Calling again should do nothing.
         */
        assert(applier.applyCommitted() == 0);

        std::cout
            << "Crash-window recovery passed\n";

        /*
         * Malformed committed commands must
         * not advance lastApplied.
         */
        log.append(
            2,
            "SET broken"
        );

        commit.advanceFollowerCommit(
            6,
            6
        );

        bool rejected = false;

        try {
            applier.applyCommitted();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);

        assert(progress.lastApplied() == 5);

        assert(store.get("broken").empty());

        std::cout
            << "Invalid committed command rejected\n";
    }

    std::cout
        << "All Raft committed-applier tests passed\n";

    return 0;
}