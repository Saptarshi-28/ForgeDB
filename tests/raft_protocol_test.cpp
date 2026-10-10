#include "raft/RaftProtocol.h"

#include <cassert>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    // 1. Encode RequestVote.

    RequestVoteRequest request{
        4,
        "node1",
        5,
        2
    };

    std::string encoded =
        encodeRequestVote(request);

    assert(
        encoded ==
        "RAFT_VOTE 4 node1 5 2\n"
    );

    // 2. Decode RequestVote.

    auto decoded =
        decodeRequestVote(encoded);

    assert(decoded.has_value());

    assert(decoded->term == 4);
    assert(decoded->candidate_id == "node1");
    assert(decoded->last_log_index == 5);
    assert(decoded->last_log_term == 2);

    std::cout
        << "RequestVote encoding passed\n";

    // 3. Reject malformed requests.

    assert(
        !decodeRequestVote(
            "RAFT_VOTE invalid node1 5 2"
        )
    );

    assert(
        !decodeRequestVote(
            "RAFT_VOTE 4 node1 5"
        )
    );

    assert(
        !decodeRequestVote(
            "RAFT_VOTE -1 node1 5 2"
        )
    );

    assert(
        !decodeRequestVote(
            "RAFT_VOTE 4 node1 5 2 extra"
        )
    );

    std::cout
        << "Invalid request tests passed\n";

    // 4. Encode/decode a granted vote.

    RequestVoteResponse response{
        4,
        true
    };

    std::string encoded_response =
        encodeRequestVoteResponse(response);

    assert(
        encoded_response ==
        "RAFT_VOTE_REPLY 4 1\n"
    );

    auto decoded_response =
        decodeRequestVoteResponse(
            encoded_response
        );

    assert(decoded_response.has_value());
    assert(decoded_response->term == 4);
    assert(decoded_response->vote_granted);

    // 5. Test a rejected vote.

    auto rejected =
        decodeRequestVoteResponse(
            "RAFT_VOTE_REPLY 5 0\n"
        );

    assert(rejected.has_value());
    assert(rejected->term == 5);
    assert(!rejected->vote_granted);

    // 6. Reject invalid responses.

    assert(
        !decodeRequestVoteResponse(
            "RAFT_VOTE_REPLY 4 7"
        )
    );

    std::cout
        << "RequestVote response tests passed\n";

    std::cout
        << "All Raft protocol tests passed\n";

    return 0;
}