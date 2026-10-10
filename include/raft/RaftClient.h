#pragma once

#include "raft/RaftMessages.h"

#include <optional>
#include <string>

namespace forgedb::raft {

class RaftClient {
public:
    std::optional<RequestVoteResponse>
    requestVote(
        const std::string& host,
        int port,
        const RequestVoteRequest& request
    );

    std::optional<AppendEntriesResponse>
    appendEntries(
        const std::string& host,
        int port,
        const AppendEntriesRequest& request
    );
};

}