#pragma once
#include "replication/ReplicationEntry.h"
#include <mutex>
#include <string>

namespace forgedb::replication {

enum class ReplicationStatus {
    ACKNOWLEDGED,
    REJECTED,
    TRANSPORT_ERROR
};

class ReplicaClient {
public:
    ReplicaClient();
    ~ReplicaClient();

    bool connectTo(
        const std::string& host,
        int port
    );

    bool replicate(
        const std::string& command
    );

    void disconnect();

    bool isConnected() const;

    ReplicationStatus replicateEntry(
        const ReplicationEntry& entry
    );

private:
    bool sendAll(
        const std::string& data
    );

    bool receiveResponse(
        std::string& response
    );

    int socket_fd_ = -1;
    bool connected_ = false;

    mutable std::mutex mutex_;
};

}