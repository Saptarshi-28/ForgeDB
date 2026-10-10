#include "raft/RaftProtocol.h"

#include <cassert>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    AppendEntriesRequest heartbeat{
        4,
        "node1",
        7,
        3,
        5
    };

    std::string encoded =
        encodeAppendEntries(
            heartbeat
        );

    assert(
        encoded ==
        "RAFT_APPEND 4 node1 7 3 5\n"
    );

    auto decoded =
        decodeAppendEntries(
            encoded
        );

    assert(decoded.has_value());

    assert(
        decoded->term == 4
    );

    assert(
        decoded->leader_id ==
        "node1"
    );

    assert(
        decoded->prev_log_index == 7
    );

    assert(
        decoded->prev_log_term == 3
    );

    assert(
        decoded->leader_commit == 5
    );

    std::cout
        << "AppendEntries encoding passed\n";

    /*
     * Empty log heartbeat.
     */
    auto empty =
        decodeAppendEntries(
            "RAFT_APPEND 1 node2 0 0 0\n"
        );

    assert(
        empty.has_value()
    );

    std::cout
        << "Empty-log heartbeat passed\n";

    /*
     * Invalid messages.
     */
    assert(
        !decodeAppendEntries(
            "RAFT_APPEND"
        )
    );

    assert(
        !decodeAppendEntries(
            "RAFT_APPEND 4 node1 7 3"
        )
    );

    assert(
        !decodeAppendEntries(
            "RAFT_APPEND -1 node1 7 3 5"
        )
    );

    assert(
        !decodeAppendEntries(
            "RAFT_APPEND 4 node1 0 3 0"
        )
    );

    assert(
        !decodeAppendEntries(
            "RAFT_APPEND 4 node1 7 3 5 extra"
        )
    );

    std::cout
        << "Invalid heartbeat tests passed\n";

    /*
     * Successful response.
     */
    AppendEntriesResponse response{
        4,
        true
    };

    std::string encoded_response =
        encodeAppendEntriesResponse(
            response
        );

    assert(
        encoded_response ==
        "RAFT_APPEND_REPLY 4 1\n"
    );

    auto decoded_response =
        decodeAppendEntriesResponse(
            encoded_response
        );

    assert(
        decoded_response.has_value()
    );

    assert(
        decoded_response->term == 4
    );

    assert(
        decoded_response->success
    );

    /*
     * Rejected heartbeat.
     */
    auto rejected =
        decodeAppendEntriesResponse(
            "RAFT_APPEND_REPLY 6 0\n"
        );

    assert(
        rejected.has_value()
    );

    assert(
        rejected->term == 6
    );

    assert(
        !rejected->success
    );

    /*
     * Invalid response.
     */
    assert(
        !decodeAppendEntriesResponse(
            "RAFT_APPEND_REPLY 4 8"
        )
    );

    std::cout
        << "AppendEntries response passed\n";

    std::cout
        << "All AppendEntries protocol tests passed\n";

    return 0;
}