#pragma once

#include "raft/RaftLog.h"

#include <cstdint>
#include <mutex>
#include <vector>

namespace forgedb::raft {

class RaftCommitState {
public:
    /*
     * Production: recover committed index
     * and make future advances durable.
     */
    explicit RaftCommitState(
        RaftLog& durable_log
    );

    /*
     * Explicit volatile mode for isolated
     * majority-calculation unit tests.
     */
    struct VolatileForTests {};

    explicit RaftCommitState(
        VolatileForTests
    );

    uint64_t commitIndex() const;

    uint64_t advanceLeaderCommit(
        uint64_t current_term,
        const std::vector<uint64_t>&
            follower_match_indexes,
        const RaftLog& log
    );
    uint64_t advanceFollowerCommit(
        uint64_t leader_commit,
        uint64_t follower_last_log_index
    );

private:
    RaftLog* durable_log_ = nullptr;
    mutable std::mutex mutex_;

    uint64_t commit_index_ = 0;
};

}