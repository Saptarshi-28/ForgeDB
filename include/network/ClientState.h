#pragma once

#include <string>
#include <deque>
#include <cstdint>

namespace forgedb::network {

struct ClientState {
    int fd;
    uint64_t generation;

    std::string receive_buffer;
    std::string output_buffer;

    std::deque<std::string> command_queue;

    bool processing = false;
    bool active = true;
};

}