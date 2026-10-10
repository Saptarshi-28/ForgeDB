#include "raft/RaftHeartbeat.h"

#include <iostream>
#include <stdexcept>

namespace forgedb::raft {

RaftHeartbeat::RaftHeartbeat(
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

    /*
     * Only the current Raft leader
     * may send leader heartbeats.
     */
    if (
        state_.role() !=
        RaftRole::LEADER
    ) {
        return result;
    }

    uint64_t leader_term =
        state_.currentTerm();

    auto [
        last_log_index,
        last_log_term
    ] =
        log_.lastLogInfo();

    AppendEntriesRequest request{
        leader_term,
        node_id_,
        last_log_index,
        last_log_term,

        /*
         * commitIndex does not exist yet.
         */
        0
    };

    for (
        const auto& peer :
        peers_
    ) {
        /*
         * Another RPC may have already
         * forced us to step down.
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

        ++result.peers_contacted;

        auto response =
            client_.appendEntries(
                peer.host,
                peer.port,
                request
            );

        if (!response.has_value()) {

            std::cout
                << "Heartbeat to "
                << peer.node_id
                << " failed\n";

            continue;
        }

        /*
         * Critical Raft rule:
         *
         * any response carrying a newer
         * term immediately invalidates
         * our leadership.
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
            response->term ==
                leader_term &&
            response->success
        ) {
            ++result.successful;

            std::cout
                << "Heartbeat acknowledged by "
                << peer.node_id
                << "\n";
        }
        else {

            std::cout
                << "Heartbeat rejected by "
                << peer.node_id
                << "\n";
        }
    }

    return result;
}

}