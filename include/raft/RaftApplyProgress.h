#pragma once

#include "raft/RaftLog.h"

#include <cstdint>
#include <mutex>
#include <string>

namespace forgedb::raft {

class RaftApplyProgress {
public:
    RaftApplyProgress(
        RaftLog& log,
        const std::string& filename
    );

    uint64_t lastApplied() const;

    // Call ONLY after the corresponding KV write
    // has completed durably.
    void recordApplied(uint64_t index);

private:
    RaftLog& log_;
    std::string filename_;

    mutable std::mutex mutex_;
    uint64_t last_applied_ = 0;
    bool failed_ = false;
};

}