#include "raft/RaftElection.h"

#include <iostream>
#include <stdexcept>

namespace forgedb::raft {

RaftElection::RaftElection(
    const std::string& node_id,
    RaftState& state,
    RaftLog& log,
    const std::vector<RaftPeer>& peers
)
    : node_id_(node_id),
      state_(state),
      log_(log),
      peers_(peers)
{
    if (node_id_.empty()) {
        throw std::invalid_argument(
            "Raft node ID cannot be empty"
        );
    }

    for (const auto& peer : peers_) {

        if (
            peer.node_id.empty() ||
            peer.host.empty() ||
            peer.port < 1 ||
            peer.port > 65535
        ) {

            throw std::invalid_argument(
                "Invalid Raft peer"
            );
        }

        /*
         * peers_ contains OTHER nodes only.
         *
         * The local node's own vote is
         * counted separately.
         */
        if (
            peer.node_id ==
            node_id_
        ) {

            throw std::invalid_argument(
                "Raft peer list contains local node"
            );
        }
    }
}

ElectionResult
RaftElection::runElection()
{
    /*
     * Atomically:
     *
     * term++
     * role = CANDIDATE
     * votedFor = ourselves
     * persist
     */
    uint64_t election_term =
        state_.startElection();

    auto [
        last_log_index,
        last_log_term
    ] =
        log_.lastLogInfo();

    const std::size_t cluster_size =
        peers_.size() + 1;

    const std::size_t majority =
        cluster_size / 2 + 1;

    /*
     * Candidate always starts with its
     * own vote.
     */
    std::size_t votes = 1;

    std::cout
        << "Starting election for term "
        << election_term
        << "\n";

    std::cout
        << "Votes needed: "
        << majority
        << " of "
        << cluster_size
        << "\n";

    /*
     * Important for a single-node cluster.
     */
    if (votes >= majority) {

        const bool won =
            state_.becomeLeaderWithNoopIfCandidate(
                election_term,
                log_
            );

        return {
            election_term,
            votes,
            cluster_size,
            won
        };
    }

    RequestVoteRequest request{
        election_term,
        node_id_,
        last_log_index,
        last_log_term
    };

    for (
        const auto& peer :
        peers_
    ) {

        /*
         * Another RPC may have already
         * moved us into a newer term.
         */
        if (
            state_.currentTerm() !=
                election_term ||
            state_.role() !=
                RaftRole::CANDIDATE
        ) {

            return {
                state_.currentTerm(),
                votes,
                cluster_size,
                false
            };
        }

        std::cout
            << "Requesting vote from "
            << peer.node_id
            << " at "
            << peer.host
            << ":"
            << peer.port
            << "\n";

        auto response =
            client_.requestVote(
                peer.host,
                peer.port,
                request
            );

        /*
         * No response is NOT a rejection.
         *
         * The peer may simply be offline.
         */
        if (!response.has_value()) {

            std::cout
                << peer.node_id
                << " did not respond\n";

            continue;
        }

        /*
         * Critical Raft rule:
         *
         * If anybody knows about a newer
         * term, we immediately abandon
         * this election and become a
         * follower.
         */
        if (
            response->term >
            election_term
        ) {

            std::cout
                << peer.node_id
                << " has newer term "
                << response->term
                << "\n";

            state_.updateTerm(
                response->term
            );

            return {
                response->term,
                votes,
                cluster_size,
                false
            };
        }

        /*
         * Ignore stale responses.
         */
        if (
            response->term !=
            election_term
        ) {
            continue;
        }

        if (
            response->vote_granted
        ) {

            ++votes;

            std::cout
                << peer.node_id
                << " granted vote. "
                << "Votes = "
                << votes
                << "\n";
        }
        else {

            std::cout
                << peer.node_id
                << " rejected vote\n";
        }

        /*
         * We only become leader if we
         * are STILL candidate in the
         * same election term.
         */
        if (votes >= majority) {

            if (
                !state_.becomeLeaderWithNoopIfCandidate(
                    election_term,
                    log_
                )
            ) {
                return {
                    state_.currentTerm(),
                    votes,
                    cluster_size,
                    false
                };
            }

            std::cout
                << node_id_
                << " won election for term "
                << election_term
                << "\n";

            return {
                election_term,
                votes,
                cluster_size,
                true
            };
        }
    }

    std::cout
        << node_id_
        << " did not obtain majority\n";

    /*
     * In real Raft, a candidate normally
     * remains candidate until:
     *
     * - another leader contacts it,
     * - a higher term appears, or
     * - its election timer expires and
     *   another election begins.
     *
     * Therefore we deliberately do NOT
     * force it back to FOLLOWER here.
     */

    return {
        election_term,
        votes,
        cluster_size,
        false
    };
}

}