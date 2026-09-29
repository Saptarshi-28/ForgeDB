#pragma once

#include <cstdint>
#include <string>

namespace forgedb::replication {

struct ReplicationEntry {
    uint64_t sequence;
    std::string command;
};

}