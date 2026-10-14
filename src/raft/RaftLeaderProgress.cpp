#include "raft/RaftLeaderProgress.h"

#include <limits>
#include <stdexcept>
#include <unordered_set>

namespace forgedb::raft {

RaftLeaderProgress::RaftLeaderProgress(
    const std::vector<RaftPeer>& peers
)
{
    std::unordered_set<std::string>
        seen;

    for (
        const auto& peer :
        peers
    ) {
        if (
            peer.node_id.empty()
        ) {
            throw std::invalid_argument(
                "Raft peer ID cannot be empty"
            );
        }

        if (
            !seen.insert(
                peer.node_id
            ).second
        ) {
            throw std::invalid_argument(
                "Duplicate Raft peer ID"
            );
        }

        progress_.emplace(
            peer.node_id,
            PeerProgress{
                1,
                0
            }
        );
    }
}

void RaftLeaderProgress::reset(
    uint64_t last_log_index
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (
        last_log_index ==
        std::numeric_limits<uint64_t>::max()
    ) {
        throw std::overflow_error(
            "Raft nextIndex overflow"
        );
    }

    const uint64_t next =
        last_log_index + 1;

    for (
        auto& [peer_id, state] :
        progress_
    ) {
        state.next_index = next;
        state.match_index = 0;
    }
}

uint64_t RaftLeaderProgress::nextIndex(
    const std::string& peer_id
) const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return
        findPeerUnlocked(
            peer_id
        ).next_index;
}

uint64_t RaftLeaderProgress::matchIndex(
    const std::string& peer_id
) const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return
        findPeerUnlocked(
            peer_id
        ).match_index;
}

PeerProgress RaftLeaderProgress::progress(
    const std::string& peer_id
) const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return
        findPeerUnlocked(
            peer_id
        );
}

std::vector<uint64_t>
RaftLeaderProgress::followerMatchIndexes() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    std::vector<uint64_t> indexes;

    indexes.reserve(
        progress_.size()
    );

    for (
        const auto& [peer_id, state] :
        progress_
    ) {
        indexes.push_back(
            state.match_index
        );
    }

    return indexes;
}

void RaftLeaderProgress::recordSuccess(
    const std::string& peer_id,
    uint64_t replicated_through
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    auto& state =
        findPeerUnlocked(
            peer_id
        );

    /*
     * Never move matchIndex backwards.
     */
    if (
        replicated_through <
        state.match_index
    ) {
        return;
    }

    if (
        replicated_through ==
        std::numeric_limits<uint64_t>::max()
    ) {
        throw std::overflow_error(
            "Raft nextIndex overflow"
        );
    }

    state.match_index =
        replicated_through;

    state.next_index =
        replicated_through + 1;
}

void RaftLeaderProgress::recordFailure(
    const std::string& peer_id
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    auto& state =
        findPeerUnlocked(
            peer_id
        );

    /*
     * nextIndex cannot go below 1.
     *
     * nextIndex == 1 means the leader
     * will send:
     *
     * prevLogIndex = 0
     * prevLogTerm  = 0
     */
    if (
        state.next_index > 1
    ) {
        --state.next_index;
    }
}

PeerProgress&
RaftLeaderProgress::findPeerUnlocked(
    const std::string& peer_id
)
{
    auto it =
        progress_.find(
            peer_id
        );

    if (
        it == progress_.end()
    ) {
        throw std::out_of_range(
            "Unknown Raft peer"
        );
    }

    return it->second;
}

const PeerProgress&
RaftLeaderProgress::findPeerUnlocked(
    const std::string& peer_id
) const
{
    auto it =
        progress_.find(
            peer_id
        );

    if (
        it == progress_.end()
    ) {
        throw std::out_of_range(
            "Unknown Raft peer"
        );
    }

    return it->second;
}

}