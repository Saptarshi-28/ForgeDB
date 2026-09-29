#include "replication/ReplicaClient.h"

#include <iostream>

int main()
{
    forgedb::replication::ReplicaClient replica;

    if (
        !replica.connectTo(
            "127.0.0.1",
            6380
        )
    ) {

        std::cerr
            << "Could not connect "
            << "to follower\n";

        return 1;
    }

    std::cout
        << "Connected to follower\n";

    if (
        !replica.replicate(
            "SET replication_probe working"
        )
    ) {

        std::cerr
            << "Replication failed\n";

        return 1;
    }

    std::cout
        << "Command replicated\n";

    return 0;
}