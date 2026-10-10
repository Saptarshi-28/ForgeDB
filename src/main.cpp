#include "network/Server.h"
#include "raft/RaftPeer.h"
#include <vector>
#include <iostream>
#include <stdexcept>
#include <string>

int main(
    int argc,
    char* argv[]
)
{
    if (argc < 4) {

        std::cerr
            << "Usage:\n"
            << "  "
            << argv[0]
            << " <port> <node_id> standalone\n"
            << "  "
            << argv[0]
            << " <port> <node_id> follower\n"
            << "  "
            << argv[0]
            << " <port> <node_id> leader "
            << "<replica_host> "
            << "<replica_port>\n"
            << "  "
            << argv[0]
            << " <port> <node_id> raft "
            << "<peer_id> <peer_host> <peer_port> ...\n";

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

    std::string node_id =
        argv[2];

    if (node_id.empty()) {

        std::cerr
            << "Node ID cannot be empty\n";

        return 1;
    }

    std::string role_argument =
        argv[3];

    forgedb::network::NodeRole role =
        forgedb::network::
            NodeRole::STANDALONE;

    std::string replica_host;

    int replica_port = 0;
    std::vector<forgedb::raft::RaftPeer> raft_peers;

    if (
        role_argument ==
        "standalone"
    ) {

        if (argc != 4) {

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

        if (argc != 4) {

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

        if (argc != 6) {

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
            argv[4];

        try {
            replica_port =
                std::stoi(
                    argv[5]
                );
        }
        catch (const std::exception&) {

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
    else if (
        role_argument ==
        "raft"
    )
    {
        /*
         * argv:
         *
         * 0 executable
         * 1 local port
         * 2 node id
         * 3 raft
         *
         * then groups of:
         * peer_id peer_host peer_port
         */
        if (
            argc < 7 ||
            (argc - 4) % 3 != 0
        )
        {
            std::cerr
                << "Raft mode requires peer groups:\n"
                << "<peer_id> "
                << "<peer_host> "
                << "<peer_port>\n";

            return 1;
        }

        role =
            forgedb::network::
                NodeRole::RAFT;

        for (
            int i = 4;
            i < argc;
            i += 3
        )
        {
            std::string peer_id =
                argv[i];

            std::string peer_host =
                argv[i + 1];

            int peer_port;

            try {
                peer_port =
                    std::stoi(
                        argv[i + 2]
                    );
            }
            catch (
                const std::exception&
            )
            {
                std::cerr
                    << "Invalid Raft peer port\n";

                return 1;
            }

            if (
                peer_port < 1 ||
                peer_port > 65535
            )
            {
                std::cerr
                    << "Raft peer port must be "
                    << "between 1 and 65535\n";

                return 1;
            }

            if (
                peer_id == node_id
            )
            {
                std::cerr
                    << "Raft peer list cannot "
                    << "contain local node\n";

                return 1;
            }

            raft_peers.push_back({
                peer_id,
                peer_host,
                peer_port
            });
        }
    }
    else {

        std::cerr
            << "Unknown role: "
            << role_argument
            << "\n";

        return 1;
    }

    forgedb::network::Server server{
        port,
        node_id,
        role,
        replica_host,
        replica_port,
        raft_peers
    };

    server.start();

    return 0;
}