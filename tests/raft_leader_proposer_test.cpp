#include "raft/RaftLeaderProposer.h"

#include <cassert>
#include <filesystem>
#include <future>
#include <iostream>
#include <set>
#include <stdexcept>
#include <vector>

namespace fs = std::filesystem;

int main()
{
    using namespace forgedb::raft;

    const fs::path dir =
        "build/raft_leader_proposer_test_data";

    fs::remove_all(dir);
    fs::create_directories(dir);

    const auto log_file = (dir / "raft.log").string();
    const auto state_file = (dir / "raft.meta").string();

    {
        RaftState state("node1", state_file);
        RaftLog log(log_file);

        state.updateTerm(7);
        state.setRole(RaftRole::LEADER);

        RaftLeaderProposer proposer(state, log);

        // Test 1: valid proposals.
        assert(proposer.propose("SET x 100") == 1);
        assert(proposer.propose("DELETE x") == 2);

        // Test 2: malformed command rejected.
        bool rejected = false;

        try {
            proposer.propose("SET missing");
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }

        assert(rejected);
        assert(log.lastLogInfo().first == 2);

        std::cout << "Validation passed\n";

        // Test 3: concurrent proposals.
        std::vector<std::future<uint64_t>> futures;

        for (int i = 0; i < 12; ++i) {
            futures.push_back(std::async(
                std::launch::async,
                [&proposer, i]() {
                    return proposer.propose(
                        "SET key" + std::to_string(i) +
                        " value" + std::to_string(i)
                    );
                }
            ));
        }

        std::set<uint64_t> indexes;

        for (auto& future : futures) {
            indexes.insert(future.get());
        }

        assert(indexes.size() == 12);
        assert(log.lastLogInfo().first == 14);

        for (uint64_t i = 3; i <= 14; ++i) {
            assert(indexes.contains(i));

            auto entry = log.entryAt(i);

            assert(entry.has_value());
            assert(entry->term == 7);
        }

        std::cout
            << "Concurrent proposal serialization passed\n";

        // Test 4: log append is NOT commitment.
        assert(log.protectedIndex() == 0);

        std::cout
            << "Uncommitted proposals remain uncommitted\n";

        // Test 5: follower cannot propose.
        state.setRole(RaftRole::FOLLOWER);

        rejected = false;

        try {
            proposer.propose("SET forbidden yes");
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);
        assert(log.lastLogInfo().first == 14);

        std::cout
            << "Follower proposal rejected\n";
    }

    // Test 6: appended records survive restart.
    {
        RaftLog recovered(log_file);

        assert(recovered.lastLogInfo().first == 14);

        auto first = recovered.entryAt(1);
        auto second = recovered.entryAt(2);

        assert(first.has_value());
        assert(second.has_value());

        assert(first->command == "SET x 100");
        assert(second->command == "DELETE x");

        assert(recovered.protectedIndex() == 0);

        std::cout
            << "Durable proposal recovery passed\n";
    }

    {
        const auto fence_log =
            (dir / "fence.log").string();

        const auto fence_meta =
            (dir / "fence.meta").string();

        RaftState state("node1", fence_meta);
        RaftLog log(fence_log);

        state.updateTerm(7);
        state.setRole(RaftRole::LEADER);

        RaftLeaderProposer proposer(state, log);

        // A legitimate leader can propose.
        assert(proposer.propose("SET before yes") == 1);

        // A newer leader is observed.
        assert(state.observeLeader(8));

        assert(state.currentTerm() == 8);
        assert(state.role() == RaftRole::FOLLOWER);

        // The old leader must no longer propose.
        bool rejected = false;

        try {
            proposer.propose("SET stale no");
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);
        assert(log.lastLogInfo().first == 1);

        // Simulate this node being elected again.
        state.setRole(RaftRole::LEADER);

        assert(proposer.propose("SET fresh yes") == 2);

        auto entry = log.entryAt(2);

        assert(entry.has_value());
        assert(entry->term == 8);
        assert(entry->command == "SET fresh yes");

        // Appending still does not commit.
        assert(log.protectedIndex() == 0);

        std::cout
            << "Leadership fencing regression passed\n";
    }

    std::cout
        << "All Raft leader-proposer tests passed\n";

    return 0;
}