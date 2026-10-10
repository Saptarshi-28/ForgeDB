#pragma once

#include <cstdint>
#include <string>

namespace forgedb::raft {

struct RaftLogEntry {

    uint64_t index;

    uint64_t term;

    std::string command;
};

}