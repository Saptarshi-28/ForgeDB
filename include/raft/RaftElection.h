#pragma once

#include "raft/RaftClient.h"
#include "raft/RaftLog.h"
#include "raft/RaftPeer.h"
#include "raft/RaftState.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace forgedb::raft {

struct ElectionResult {
    uint64_t term;

    std::size_t votes_received;

    std::size_t cluster_size;

    bool won;
};

class RaftElection {
public:
    RaftElection(
        const std::string& node_id,
        RaftState& state,
        RaftLog& log,
        const std::vector<RaftPeer>& peers
    );

    ElectionResult runElection();

private:
    std::string node_id_;

    RaftState& state_;

    RaftLog& log_;

    std::vector<RaftPeer> peers_;

    RaftClient client_;
};

}