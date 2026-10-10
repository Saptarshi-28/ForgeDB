#pragma once

#include "raft/RaftMessages.h"

#include <optional>
#include <string>

namespace forgedb::raft {

std::string encodeRequestVote(
    const RequestVoteRequest& request
);

std::optional<RequestVoteRequest>
decodeRequestVote(
    const std::string& message
);

std::string encodeRequestVoteResponse(
    const RequestVoteResponse& response
);

std::optional<RequestVoteResponse>
decodeRequestVoteResponse(
    const std::string& message
);

std::string encodeAppendEntries(
    const AppendEntriesRequest& request
);

std::optional<AppendEntriesRequest>
decodeAppendEntries(
    const std::string& message
);

std::string encodeAppendEntriesResponse(
    const AppendEntriesResponse& response
);

std::optional<AppendEntriesResponse>
decodeAppendEntriesResponse(
    const std::string& message
);
}