#pragma once

#include "raft/RaftPeer.h"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace forgedb::raft {

struct PeerProgress {
    uint64_t next_index;
    uint64_t match_index;
};

class RaftLeaderProgress {
public:

    explicit RaftLeaderProgress(
        const std::vector<RaftPeer>& peers
    );

    void reset(
        uint64_t last_log_index
    );

    uint64_t nextIndex(
        const std::string& peer_id
    ) const;

    uint64_t matchIndex(
        const std::string& peer_id
    ) const;

    PeerProgress progress(
        const std::string& peer_id
    ) const;

    void recordSuccess(
        const std::string& peer_id,
        uint64_t replicated_through
    );

    void recordFailure(
        const std::string& peer_id
    );

    std::vector<uint64_t> followerMatchIndexes() const;

private:

    PeerProgress&
    findPeerUnlocked(
        const std::string& peer_id
    );

    const PeerProgress&
    findPeerUnlocked(
        const std::string& peer_id
    ) const;

    mutable std::mutex mutex_;

    std::unordered_map<
        std::string,
        PeerProgress
    > progress_;
};

}