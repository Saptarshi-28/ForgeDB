#pragma once

#include <cstdint>
#include <string>

namespace forgedb::raft {

struct RequestVoteRequest {
    uint64_t term;

    std::string candidate_id;

    uint64_t last_log_index;

    uint64_t last_log_term;
};

struct RequestVoteResponse {
    uint64_t term;

    bool vote_granted;
};

struct AppendEntriesRequest {
    uint64_t term;

    std::string leader_id;

    uint64_t prev_log_index;

    uint64_t prev_log_term;

    uint64_t leader_commit;
};

struct AppendEntriesResponse {
    uint64_t term;

    bool success;
};
}