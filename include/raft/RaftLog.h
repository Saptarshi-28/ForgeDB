#pragma once

#include "raft/RaftLogEntry.h"
#include <optional>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace forgedb::raft {

class RaftLog {
public:

    explicit RaftLog(
        const std::string& filename
    );

    RaftLogEntry append(
        uint64_t term,
        const std::string& command
    );

    std::optional<uint64_t>
    termAt(
        uint64_t index
    ) const;

    std::vector<RaftLogEntry> load() const;

    std::pair<uint64_t, uint64_t>
    lastLogInfo() const;

private:

    std::string filename_;

    mutable std::mutex mutex_;

    uint64_t next_index_ = 1;
    bool append_failed_ = false;

    std::vector<RaftLogEntry> entries_;
};

}