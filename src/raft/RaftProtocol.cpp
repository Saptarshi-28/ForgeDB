#include "raft/RaftProtocol.h"
#include <optional>
#include <charconv>
#include <cstdint>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>

namespace forgedb::raft {

namespace {

char hexDigit(
    unsigned int value
)
{
    if (value < 10) {
        return static_cast<char>(
            '0' + value
        );
    }

    return static_cast<char>(
        'A' + (value - 10)
    );
}

std::string encodeHex(
    const std::string& input
)
{
    std::string output;

    output.reserve(
        input.size() * 2
    );

    for (
        unsigned char character :
        input
    ) {
        output.push_back(
            hexDigit(
                character >> 4
            )
        );

        output.push_back(
            hexDigit(
                character & 0x0F
            )
        );
    }

    return output;
}

int hexValue(
    char character
)
{
    if (
        character >= '0' &&
        character <= '9'
    ) {
        return character - '0';
    }

    if (
        character >= 'A' &&
        character <= 'F'
    ) {
        return character - 'A' + 10;
    }

    if (
        character >= 'a' &&
        character <= 'f'
    ) {
        return character - 'a' + 10;
    }

    return -1;
}

std::optional<std::string> decodeHex(
    const std::string& input
)
{
    if (
        input.empty() ||
        input.size() % 2 != 0
    ) {
        return std::nullopt;
    }

    std::string output;

    output.reserve(
        input.size() / 2
    );

    for (
        std::size_t i = 0;
        i < input.size();
        i += 2
    ) {
        int high =
            hexValue(
                input[i]
            );

        int low =
            hexValue(
                input[i + 1]
            );

        if (
            high < 0 ||
            low < 0
        ) {
            return std::nullopt;
        }

        output.push_back(
            static_cast<char>(
                (high << 4) |
                low
            )
        );
    }

    return output;
}

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
    std::ostringstream stream;

    stream
        << "RAFT_APPEND "
        << request.term
        << " "
        << request.leader_id
        << " "
        << request.prev_log_index
        << " "
        << request.prev_log_term
        << " "
        << request.leader_commit
        << " "
        << request.entries.size();

    for (
        const auto& entry :
        request.entries
    ) {
        stream
            << " "
            << entry.index
            << " "
            << entry.term
            << " "
            << encodeHex(
                entry.command
            );
    }

    stream << "\n";

    return stream.str();
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

    AppendEntriesRequest request{};

    std::size_t entry_count = 0;

    if (
        !(stream >> marker) ||
        marker != "RAFT_APPEND"
    ) {
        return std::nullopt;
    }

    if (
        !(stream >> request.term) ||
        !(stream >> request.leader_id) ||
        !(stream >> request.prev_log_index) ||
        !(stream >> request.prev_log_term) ||
        !(stream >> request.leader_commit) ||
        !(stream >> entry_count)
    ) {
        return std::nullopt;
    }

    /*
     * Index zero represents the
     * imaginary entry before the log.
     *
     * It therefore must have term zero.
     */
    if (
        request.prev_log_index == 0 &&
        request.prev_log_term != 0
    ) {
        return std::nullopt;
    }

    request.entries.reserve(
        entry_count
    );

    uint64_t expected_index =
        request.prev_log_index + 1;

    for (
        std::size_t i = 0;
        i < entry_count;
        ++i
    ) {
        RaftLogEntry entry{};

        std::string encoded_command;

        if (
            !(stream >> entry.index) ||
            !(stream >> entry.term) ||
            !(stream >> encoded_command)
        ) {
            return std::nullopt;
        }

        auto decoded =
            decodeHex(
                encoded_command
            );

        if (
            !decoded.has_value()
        ) {
            return std::nullopt;
        }

        entry.command =
            std::move(
                *decoded
            );

        /*
         * Entries inside one AppendEntries
         * RPC must be contiguous.
         */
        if (
            entry.index !=
            expected_index
        ) {
            return std::nullopt;
        }

        request.entries.push_back(
            std::move(entry)
        );

        ++expected_index;
    }

    /*
     * Reject unexpected extra fields.
     */
    std::string extra;

    if (
        stream >> extra
    ) {
        return std::nullopt;
    }

    return request;
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