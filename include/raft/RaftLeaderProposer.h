#pragma once

#include "raft/RaftLog.h"
#include "raft/RaftState.h"

#include <cstdint>
#include <mutex>
#include <string>

namespace forgedb::raft {

class RaftLeaderProposer {
public:
    RaftLeaderProposer(
        RaftState& state,
        RaftLog& log
    );

    // Returns an appended index, NOT a commit.
    uint64_t propose(const std::string& command);

private:
    RaftState& state_;
    RaftLog& log_;

    // Serializes concurrent local proposals.
    std::mutex mutex_;
};

}