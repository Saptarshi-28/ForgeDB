#include "raft/RaftClient.h"

#include <iostream>

int main()
{
    forgedb::raft::RaftClient client;

    forgedb::raft::RequestVoteRequest
        request{
            1,
            "node1",
            0,
            0
        };

    auto response =
        client.requestVote(
            "127.0.0.1",
            6380,
            request
        );

    if (!response.has_value()) {

        std::cerr
            << "RequestVote RPC failed\n";

        return 1;
    }

    std::cout
        << "Response term: "
        << response->term
        << "\n";

    std::cout
        << "Vote granted: "
        << (
            response->vote_granted
                ? "yes"
                : "no"
        )
        << "\n";

    return 0;
}