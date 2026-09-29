#include "network/Server.h"

#include <iostream>
#include <stdexcept>
#include <string>

int main(
    int argc,
    char* argv[]
)
{
    if (
        argc != 1 &&
        argc != 2 &&
        argc != 4
    ) {

        std::cerr
            << "Usage: "
            << argv[0]
            << " [port] "
            << "[replica_host replica_port]\n";

        return 1;
    }

    int port = 6379;

    std::string replica_host;
    int replica_port = 0;

    try {

        if (argc >= 2) {
            port = std::stoi(
                argv[1]
            );
        }

        if (argc == 4) {

            replica_host =
                argv[2];

            replica_port =
                std::stoi(
                    argv[3]
                );
        }
    }
    catch (const std::exception&) {

        std::cerr
            << "Invalid port argument\n";

        return 1;
    }

    if (
        port < 1 ||
        port > 65535
    ) {

        std::cerr
            << "Server port must be "
            << "between 1 and 65535\n";

        return 1;
    }

    if (
        replica_port < 0 ||
        replica_port > 65535
    ) {

        std::cerr
            << "Replica port must be "
            << "between 1 and 65535\n";

        return 1;
    }

    forgedb::network::Server server{
        port,
        replica_host,
        replica_port
    };

    server.start();

    return 0;
}