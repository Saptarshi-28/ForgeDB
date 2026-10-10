#include "raft/RaftClient.h"

#include <iostream>

int main()
{
    using namespace forgedb::raft;

    RaftClient client;

    /*
     * Empty-log heartbeat from node1
     * in term 1.
     */
    AppendEntriesRequest request{
        1,
        "node1",
        0,
        0,
        0
    };

    auto response =
        client.appendEntries(
            "127.0.0.1",
            6380,
            request
        );

    if (!response.has_value()) {

        std::cerr
            << "AppendEntries RPC failed\n";

        return 1;
    }

    std::cout
        << "Response term: "
        << response->term
        << "\n";

    std::cout
        << "Append accepted: "
        << (
            response->success
                ? "yes"
                : "no"
        )
        << "\n";

    return 0;
}