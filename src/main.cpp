#include "network/Server.h"

#include <iostream>
#include <stdexcept>
#include <string>

int main(
    int argc,
    char* argv[]
)
{
    if (argc < 2) {

        std::cerr
            << "Usage:\n"
            << "  "
            << argv[0]
            << " <port> standalone\n"
            << "  "
            << argv[0]
            << " <port> follower\n"
            << "  "
            << argv[0]
            << " <port> leader "
            << "<replica_host> "
            << "<replica_port>\n";

        return 1;
    }

    int port;

    try {
        port =
            std::stoi(
                argv[1]
            );
    }
    catch (const std::exception&) {

        std::cerr
            << "Invalid server port\n";

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

    forgedb::network::NodeRole role =
        forgedb::network::
            NodeRole::STANDALONE;

    std::string replica_host;

    int replica_port = 0;

    if (argc >= 3) {

        std::string role_argument =
            argv[2];

        if (
            role_argument ==
            "standalone"
        ) {

            if (argc != 3) {

                std::cerr
                    << "Standalone mode "
                    << "takes no replica arguments\n";

                return 1;
            }

            role =
                forgedb::network::
                    NodeRole::STANDALONE;
        }
        else if (
            role_argument ==
            "follower"
        ) {

            if (argc != 3) {

                std::cerr
                    << "Follower mode "
                    << "takes no replica arguments\n";

                return 1;
            }

            role =
                forgedb::network::
                    NodeRole::FOLLOWER;
        }
        else if (
            role_argument ==
            "leader"
        ) {

            if (argc != 5) {

                std::cerr
                    << "Leader mode requires "
                    << "<replica_host> "
                    << "<replica_port>\n";

                return 1;
            }

            role =
                forgedb::network::
                    NodeRole::LEADER;

            replica_host =
                argv[3];

            try {
                replica_port =
                    std::stoi(
                        argv[4]
                    );
            }
            catch (
                const std::exception&
            ) {

                std::cerr
                    << "Invalid replica port\n";

                return 1;
            }

            if (
                replica_port < 1 ||
                replica_port > 65535
            ) {

                std::cerr
                    << "Replica port must be "
                    << "between 1 and 65535\n";

                return 1;
            }
        }
        else {

            std::cerr
                << "Unknown role: "
                << role_argument
                << "\n";

            return 1;
        }
    }

    forgedb::network::Server server{
        port,
        role,
        replica_host,
        replica_port
    };

    server.start();

    return 0;
}