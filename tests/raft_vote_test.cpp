#include "raft/RaftState.h"

#include <cassert>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    {
        RaftState node(
            "node1",
            "raft_vote_test.meta"
        );

        node.updateTerm(3);

        // Assume node1's latest log entry:
        // index = 5, term = 2.

        // Test 1: Old term.
        auto old_term = node.handleRequestVote(
            {2, "node2", 5, 2},
            5,
            2
        );

        assert(!old_term.vote_granted);
        assert(old_term.term == 3);

        // Test 2: Candidate has an outdated log.
        auto stale_log = node.handleRequestVote(
            {3, "node2", 4, 2},
            5,
            2
        );

        assert(!stale_log.vote_granted);

        // Test 3: Valid candidate.
        auto valid = node.handleRequestVote(
            {3, "node2", 5, 2},
            5,
            2
        );

        assert(valid.vote_granted);

        // Test 4: Same candidate retries.
        auto duplicate = node.handleRequestVote(
            {3, "node2", 5, 2},
            5,
            2
        );

        assert(duplicate.vote_granted);

        // Test 5: Different candidate, same term.
        auto different = node.handleRequestVote(
            {3, "node3", 6, 2},
            5,
            2
        );

        assert(!different.vote_granted);

        // Test 6: Higher term, but outdated log.
        auto higher_term = node.handleRequestVote(
            {4, "node3", 1, 1},
            5,
            2
        );

        assert(!higher_term.vote_granted);
        assert(higher_term.term == 4);

        // Test 7: Valid vote in the new term.
        auto new_vote = node.handleRequestVote(
            {4, "node3", 6, 2},
            5,
            2
        );

        assert(new_vote.vote_granted);

        std::cout
            << "Voting rules passed\n";
    }

    /*
     * Simulate restarting the node.
     */
    RaftState restarted(
        "node1",
        "raft_vote_test.meta"
    );

    assert(
        restarted.currentTerm() == 4
    );

    assert(
        restarted.votedFor() == "node3"
    );

    assert(
        restarted.role() ==
            RaftRole::FOLLOWER
    );

    // A restarted node must not vote for
    // another candidate in the same term.
    auto after_restart =
        restarted.handleRequestVote(
            {4, "node2", 6, 2},
            5,
            2
        );

    assert(!after_restart.vote_granted);

    std::cout
        << "Vote persistence passed\n";

    std::cout
        << "All RequestVote tests passed\n";

    return 0;
}