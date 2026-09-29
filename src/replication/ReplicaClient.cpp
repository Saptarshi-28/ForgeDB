#include "replication/ReplicaClient.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace forgedb::replication {

ReplicaClient::ReplicaClient() = default;

ReplicaClient::~ReplicaClient()
{
    disconnect();
}

bool ReplicaClient::connectTo(
    const std::string& host,
    int port
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }

    connected_ = false;

    socket_fd_ = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (socket_fd_ < 0) {
        std::cerr
            << "Replica socket creation failed\n";

        return false;
    }

    sockaddr_in address{};

    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    if (
        inet_pton(
            AF_INET,
            host.c_str(),
            &address.sin_addr
        ) != 1
    ) {

        std::cerr
            << "Invalid replica address\n";

        close(socket_fd_);
        socket_fd_ = -1;

        return false;
    }

    if (
        connect(
            socket_fd_,
            reinterpret_cast<sockaddr*>(
                &address
            ),
            sizeof(address)
        ) < 0
    ) {

        std::cerr
            << "Replica connection failed: "
            << std::strerror(errno)
            << "\n";

        close(socket_fd_);
        socket_fd_ = -1;

        return false;
    }

    connected_ = true;

    return true;
}

bool ReplicaClient::sendAll(
    const std::string& data
)
{
    std::size_t total_sent = 0;

    while (total_sent < data.size()) {

        ssize_t sent = send(
            socket_fd_,
            data.data() + total_sent,
            data.size() - total_sent,
            MSG_NOSIGNAL
        );

        if (sent < 0) {

            if (errno == EINTR) {
                continue;
            }

            return false;
        }

        if (sent == 0) {
            return false;
        }

        total_sent +=
            static_cast<std::size_t>(sent);
    }

    return true;
}

bool ReplicaClient::receiveResponse(
    std::string& response
)
{
    response.clear();

    char buffer[1024];

    while (true) {

        ssize_t received = recv(
            socket_fd_,
            buffer,
            sizeof(buffer),
            0
        );

        if (received < 0) {

            if (errno == EINTR) {
                continue;
            }

            return false;
        }

        if (received == 0) {
            return false;
        }

        response.append(
            buffer,
            static_cast<std::size_t>(
                received
            )
        );

        std::size_t newline =
            response.find('\n');

        if (newline != std::string::npos) {

            response.erase(
                newline + 1
            );

            return true;
        }
    }
}

bool ReplicaClient::replicate(
    const std::string& command
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (!connected_) {
        return false;
    }

    std::string request =
        command + "\n";

    if (!sendAll(request)) {

        connected_ = false;

        close(socket_fd_);
        socket_fd_ = -1;

        return false;
    }

    std::string response;

    if (!receiveResponse(response)) {

        connected_ = false;

        close(socket_fd_);
        socket_fd_ = -1;

        return false;
    }

    return true;
}

void ReplicaClient::disconnect()
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }

    connected_ = false;
}

bool ReplicaClient::isConnected() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return connected_;
}

ReplicationStatus ReplicaClient::replicateEntry(
    const ReplicationEntry& entry
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (!connected_) {
        return ReplicationStatus::TRANSPORT_ERROR;
    }

    std::string request =
        "REPL " +
        std::to_string(entry.sequence) +
        " " +
        entry.command +
        "\n";

    if (!sendAll(request)) {

        connected_ = false;

        close(socket_fd_);
        socket_fd_ = -1;

        return ReplicationStatus::TRANSPORT_ERROR;
    }

    std::string response;

    if (!receiveResponse(response)) {

        connected_ = false;

        close(socket_fd_);
        socket_fd_ = -1;

        return ReplicationStatus::TRANSPORT_ERROR;
    }

    std::string expected =
        "REPL_OK " +
        std::to_string(entry.sequence) +
        "\n";

    if (response == expected) {
        return ReplicationStatus::ACKNOWLEDGED;
    }

    if (
        response.rfind(
            "REPL_ERR",
            0
        ) == 0
    ) {

        std::cerr
            << "Replica rejected entry "
            << entry.sequence
            << ": "
            << response;

        return ReplicationStatus::REJECTED;
    }

    std::cerr
        << "Unexpected replica response: "
        << response;

    return ReplicationStatus::REJECTED;
}
}