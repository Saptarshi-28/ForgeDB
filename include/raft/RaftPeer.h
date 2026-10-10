#pragma once

#include <string>

namespace forgedb::raft {

struct RaftPeer {
    std::string node_id;
    std::string host;
    int port;
};

}