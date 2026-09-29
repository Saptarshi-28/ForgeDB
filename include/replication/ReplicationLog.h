#pragma once

#include "replication/ReplicationEntry.h"

#include <mutex>
#include <string>
#include <vector>

namespace forgedb::replication {

class ReplicationLog {
public:
    explicit ReplicationLog(
        const std::string& filename
    );

    ReplicationEntry append(
        const std::string& command
    );

    std::vector<ReplicationEntry>
    load() const;

private:
    std::string filename_;
    uint64_t next_sequence_ = 1;
    mutable std::mutex mutex_;
};

}