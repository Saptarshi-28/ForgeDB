#pragma once

#include "raft/RaftClient.h"
#include "raft/RaftLog.h"
#include "raft/RaftPeer.h"
#include "raft/RaftState.h"
#include "raft/RaftLeaderProgress.h"
#include <cstddef>
#include <string>
#include <vector>

namespace forgedb::raft {

struct HeartbeatResult {
    std::size_t peers_contacted;
    std::size_t successful;
    bool stepped_down;
};

class RaftHeartbeat {
public:
    RaftHeartbeat(
        const std::string& node_id,
        RaftState& state,
        RaftLog& log,
        const std::vector<RaftPeer>& peers,
        RaftLeaderProgress& progress
    );

    HeartbeatResult sendHeartbeats(
        uint64_t leader_commit = 0
    );

private:
    std::string node_id_;
    RaftLeaderProgress& progress_;

    RaftState& state_;

    RaftLog& log_;

    std::vector<RaftPeer> peers_;

    RaftClient client_;
};

}