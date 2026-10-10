#include "raft/RaftProtocol.h"

#include <charconv>
#include <cstdint>
#include <sstream>
#include <string>
#include <system_error>

namespace forgedb::raft {

namespace {

bool parseUnsigned(
    const std::string& token,
    uint64_t& value
)
{
    const char* begin = token.data();
    const char* end = begin + token.size();

    auto [ptr, error] =
        std::from_chars(
            begin,
            end,
            value
        );

    return
        error == std::errc{} &&
        ptr == end;
}

}

std::string encodeRequestVote(
    const RequestVoteRequest& request
)
{
    return
        "RAFT_VOTE " +
        std::to_string(request.term) +
        " " +
        request.candidate_id +
        " " +
        std::to_string(request.last_log_index) +
        " " +
        std::to_string(request.last_log_term) +
        "\n";
}

std::optional<RequestVoteRequest>
decodeRequestVote(
    const std::string& message
)
{
    std::istringstream stream(message);

    std::string marker;
    std::string term_token;
    std::string candidate_id;
    std::string index_token;
    std::string log_term_token;
    std::string extra;

    if (!(
        stream >>
        marker >>
        term_token >>
        candidate_id >>
        index_token >>
        log_term_token
    )) {
        return std::nullopt;
    }

    if (
        marker != "RAFT_VOTE" ||
        stream >> extra
    ) {
        return std::nullopt;
    }

    uint64_t term = 0;
    uint64_t index = 0;
    uint64_t log_term = 0;

    if (
        !parseUnsigned(term_token, term) ||
        !parseUnsigned(index_token, index) ||
        !parseUnsigned(log_term_token, log_term)
    ) {
        return std::nullopt;
    }

    if (
        term == 0 ||
        candidate_id.empty()
    ) {
        return std::nullopt;
    }

    return RequestVoteRequest{
        term,
        candidate_id,
        index,
        log_term
    };
}

std::string encodeRequestVoteResponse(
    const RequestVoteResponse& response
)
{
    return
        "RAFT_VOTE_REPLY " +
        std::to_string(response.term) +
        " " +
        (response.vote_granted ? "1" : "0") +
        "\n";
}

std::optional<RequestVoteResponse>
decodeRequestVoteResponse(
    const std::string& message
)
{
    std::istringstream stream(message);

    std::string marker;
    std::string term_token;
    std::string granted_token;
    std::string extra;

    if (!(
        stream >>
        marker >>
        term_token >>
        granted_token
    )) {
        return std::nullopt;
    }

    if (
        marker != "RAFT_VOTE_REPLY" ||
        stream >> extra
    ) {
        return std::nullopt;
    }

    uint64_t term = 0;

    if (
        !parseUnsigned(term_token, term) ||
        (
            granted_token != "0" &&
            granted_token != "1"
        )
    ) {
        return std::nullopt;
    }

    return RequestVoteResponse{
        term,
        granted_token == "1"
    };
}
std::string encodeAppendEntries(
    const AppendEntriesRequest& request
)
{
    return
        "RAFT_APPEND " +
        std::to_string(request.term) +
        " " +
        request.leader_id +
        " " +
        std::to_string(
            request.prev_log_index
        ) +
        " " +
        std::to_string(
            request.prev_log_term
        ) +
        " " +
        std::to_string(
            request.leader_commit
        ) +
        "\n";
}

std::optional<AppendEntriesRequest>
decodeAppendEntries(
    const std::string& message
)
{
    std::istringstream stream(
        message
    );

    std::string marker;
    std::string term_token;
    std::string leader_id;
    std::string index_token;
    std::string log_term_token;
    std::string commit_token;
    std::string extra;

    if (!(
        stream >>
        marker >>
        term_token >>
        leader_id >>
        index_token >>
        log_term_token >>
        commit_token
    )) {
        return std::nullopt;
    }

    if (
        marker != "RAFT_APPEND" ||
        stream >> extra
    ) {
        return std::nullopt;
    }

    uint64_t term = 0;
    uint64_t previous_index = 0;
    uint64_t previous_term = 0;
    uint64_t leader_commit = 0;

    if (
        !parseUnsigned(
            term_token,
            term
        ) ||
        !parseUnsigned(
            index_token,
            previous_index
        ) ||
        !parseUnsigned(
            log_term_token,
            previous_term
        ) ||
        !parseUnsigned(
            commit_token,
            leader_commit
        )
    ) {
        return std::nullopt;
    }

    if (
        term == 0 ||
        leader_id.empty()
    ) {
        return std::nullopt;
    }

    /*
     * The empty-log sentinel is:
     *
     * prev_log_index = 0
     * prev_log_term  = 0
     *
     * index zero with a non-zero term
     * makes no sense.
     */
    if (
        previous_index == 0 &&
        previous_term != 0
    ) {
        return std::nullopt;
    }

    return AppendEntriesRequest{
        term,
        leader_id,
        previous_index,
        previous_term,
        leader_commit
    };
}

std::string encodeAppendEntriesResponse(
    const AppendEntriesResponse& response
)
{
    return
        "RAFT_APPEND_REPLY " +
        std::to_string(
            response.term
        ) +
        " " +
        (
            response.success
                ? "1"
                : "0"
        ) +
        "\n";
}

std::optional<AppendEntriesResponse>
decodeAppendEntriesResponse(
    const std::string& message
)
{
    std::istringstream stream(
        message
    );

    std::string marker;
    std::string term_token;
    std::string success_token;
    std::string extra;

    if (!(
        stream >>
        marker >>
        term_token >>
        success_token
    )) {
        return std::nullopt;
    }

    if (
        marker !=
            "RAFT_APPEND_REPLY" ||
        stream >> extra
    ) {
        return std::nullopt;
    }

    uint64_t term = 0;

    if (
        !parseUnsigned(
            term_token,
            term
        ) ||
        (
            success_token != "0" &&
            success_token != "1"
        )
    ) {
        return std::nullopt;
    }

    return AppendEntriesResponse{
        term,
        success_token == "1"
    };
}
}