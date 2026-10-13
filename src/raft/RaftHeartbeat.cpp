#include "raft/RaftHeartbeat.h"
#include <iostream>
#include <stdexcept>
#include <iostream>
#include <stdexcept>

namespace forgedb::raft {

RaftHeartbeat::RaftHeartbeat(
    const std::string& node_id,
    RaftState& state,
    RaftLog& log,
    const std::vector<RaftPeer>& peers,
    RaftLeaderProgress& progress
)
    : node_id_(node_id),
      state_(state),
      log_(log),
      peers_(peers),
      progress_(progress)
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

HeartbeatResult
RaftHeartbeat::sendHeartbeats()
{
    HeartbeatResult result{
        0,
        0,
        false
    };

    if (
        state_.role() !=
        RaftRole::LEADER
    ) {
        return result;
    }

    const uint64_t leader_term =
        state_.currentTerm();

    for (
        const auto& peer :
        peers_
    ) {
        /*
         * We may have stepped down while
         * talking to an earlier peer.
         */
        if (
            state_.role() !=
                RaftRole::LEADER ||
            state_.currentTerm() !=
                leader_term
        ) {
            result.stepped_down = true;

            return result;
        }

        const uint64_t next_index =
            progress_.nextIndex(
                peer.node_id
            );

        const uint64_t prev_index =
            next_index - 1;

        auto prev_term =
            log_.termAt(
                prev_index
            );

        if (
            !prev_term.has_value()
        ) {
            throw std::runtime_error(
                "Invalid Raft nextIndex"
            );
        }

        /*
         * Everything from nextIndex onward
         * is currently missing from this
         * follower according to the leader.
         */
        auto entries =
            log_.entriesFrom(
                next_index
            );

        AppendEntriesRequest request{
            leader_term,
            node_id_,
            prev_index,
            *prev_term,
            0,
            entries
        };

        ++result.peers_contacted;

        auto response =
            client_.appendEntries(
                peer.host,
                peer.port,
                request
            );

        /*
         * Transport failure tells us
         * nothing about the follower log.
         *
         * Do NOT decrement nextIndex.
         */
        if (
            !response.has_value()
        ) {
            std::cerr
                << "Replication to "
                << peer.node_id
                << " failed\n";

            continue;
        }

        /*
         * Any higher term immediately
         * invalidates our leadership.
         */
        if (
            response->term >
            leader_term
        ) {
            std::cout
                << peer.node_id
                << " reported newer term "
                << response->term
                << ". Stepping down.\n";

            state_.updateTerm(
                response->term
            );

            result.stepped_down = true;

            return result;
        }

        if (
            response->success
        ) {
            uint64_t replicated_through =
                prev_index;

            if (
                !entries.empty()
            ) {
                replicated_through =
                    entries.back().index;
            }

            progress_.recordSuccess(
                peer.node_id,
                replicated_through
            );

            ++result.successful;

            if (
                entries.empty()
            ) {
                std::cout
                    << "Heartbeat acknowledged by "
                    << peer.node_id
                    << "\n";
            }
            else {
                std::cout
                    << "Replicated through index "
                    << replicated_through
                    << " to "
                    << peer.node_id
                    << "\n";
            }

            continue;
        }

        /*
         * The follower rejected our
         * prevLogIndex / prevLogTerm.
         *
         * Back up one position.
         */
        progress_.recordFailure(
            peer.node_id
        );

        std::cout
            << peer.node_id
            << " rejected log prefix; "
            << "nextIndex -> "
            << progress_.nextIndex(
                peer.node_id
            )
            << "\n";
    }

    return result;
}

}