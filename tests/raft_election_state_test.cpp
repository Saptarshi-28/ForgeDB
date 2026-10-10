#include "raft/RaftState.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    const std::string filename =
        "build/raft_election_state.meta";

    std::filesystem::remove(filename);
    std::filesystem::remove(
        filename + ".tmp"
    );

    {
        RaftState state(
            "node1",
            filename
        );

        assert(
            state.currentTerm() == 0
        );

        assert(
            state.role() ==
            RaftRole::FOLLOWER
        );

        uint64_t term =
            state.startElection();

        assert(term == 1);

        assert(
            state.currentTerm() == 1
        );

        assert(
            state.role() ==
            RaftRole::CANDIDATE
        );

        assert(
            state.votedFor() ==
            "node1"
        );

        std::cout
            << "Election start passed\n";
    }

    /*
     * Restart the node.
     *
     * Persistent state survives,
     * but role returns to FOLLOWER.
     */
    {
        RaftState restarted(
            "node1",
            filename
        );

        assert(
            restarted.currentTerm() == 1
        );

        assert(
            restarted.votedFor() ==
            "node1"
        );

        assert(
            restarted.role() ==
            RaftRole::FOLLOWER
        );

        uint64_t term =
            restarted.startElection();

        assert(term == 2);

        assert(
            restarted.role() ==
            RaftRole::CANDIDATE
        );

        assert(
            restarted.votedFor() ==
            "node1"
        );

        std::cout
            << "Election restart passed\n";
    }
    {
        const std::string leader_file =
            "build/raft_leader_observation.meta";

        std::filesystem::remove(
            leader_file
        );

        std::filesystem::remove(
            leader_file + ".tmp"
        );

        RaftState state(
            "node2",
            leader_file
        );

        /*
         * node2 becomes candidate in term 1.
         */
        assert(
            state.startElection() == 1
        );

        assert(
            state.role() ==
            RaftRole::CANDIDATE
        );

        /*
         * Valid leader in the same term:
         * candidate must step down.
         */
        assert(
            state.observeLeader(1)
        );

        assert(
            state.role() ==
            RaftRole::FOLLOWER
        );

        assert(
            state.currentTerm() == 1
        );

        /*
         * Become candidate in term 2.
         */
        assert(
            state.startElection() == 2
        );

        /*
         * Old leader from term 1
         * must be rejected.
         */
        assert(
            !state.observeLeader(1)
        );

        assert(
            state.currentTerm() == 2
        );

        assert(
            state.role() ==
            RaftRole::CANDIDATE
        );

        /*
         * New leader term 3 wins.
         */
        assert(
            state.observeLeader(3)
        );

        assert(
            state.currentTerm() == 3
        );

        assert(
            state.role() ==
            RaftRole::FOLLOWER
        );

        assert(
            state.votedFor().empty()
        );

        std::cout
            << "Leader observation passed\n";
    }

    std::cout
        << "All election state tests passed\n";

    return 0;
}