#include "raft/RaftClient.h"
#include "raft/RaftProtocol.h"
#include <fcntl.h>
#include <poll.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <cerrno>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace forgedb::raft {

namespace {

bool sendAll(
    int socket_fd,
    const std::string& data
)
{
    std::size_t total_sent = 0;

    while (
        total_sent <
        data.size()
    ) {

        ssize_t sent = send(
            socket_fd,
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
            static_cast<std::size_t>(
                sent
            );
    }

    return true;
}
constexpr int RAFT_RPC_TIMEOUT_MS = 500;

bool connectWithTimeout(
    int socket_fd,
    const sockaddr* address,
    socklen_t address_length
)
{
    int original_flags =
        fcntl(
            socket_fd,
            F_GETFL,
            0
        );

    if (original_flags < 0) {
        return false;
    }

    if (
        fcntl(
            socket_fd,
            F_SETFL,
            original_flags | O_NONBLOCK
        ) < 0
    ) {
        return false;
    }

    int result =
        connect(
            socket_fd,
            address,
            address_length
        );

    if (result == 0) {

        fcntl(
            socket_fd,
            F_SETFL,
            original_flags
        );

        return true;
    }

    if (errno != EINPROGRESS) {
        return false;
    }

    pollfd descriptor{};

    descriptor.fd =
        socket_fd;

    descriptor.events =
        POLLOUT;

    while (true) {

        result =
            poll(
                &descriptor,
                1,
                RAFT_RPC_TIMEOUT_MS
            );

        if (
            result < 0 &&
            errno == EINTR
        ) {
            continue;
        }

        break;
    }

    if (result <= 0) {
        return false;
    }

    int socket_error = 0;

    socklen_t error_length =
        sizeof(socket_error);

    if (
        getsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_ERROR,
            &socket_error,
            &error_length
        ) < 0
    ) {
        return false;
    }

    if (socket_error != 0) {

        errno = socket_error;

        return false;
    }

    if (
        fcntl(
            socket_fd,
            F_SETFL,
            original_flags
        ) < 0
    ) {
        return false;
    }

    return true;
}
bool setSocketTimeouts(
    int socket_fd
)
{
    timeval timeout{};

    timeout.tv_sec = 0;

    timeout.tv_usec =
        RAFT_RPC_TIMEOUT_MS * 1000;

    if (
        setsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)
        ) < 0
    ) {
        return false;
    }

    if (
        setsockopt(
            socket_fd,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            sizeof(timeout)
        ) < 0
    ) {
        return false;
    }

    return true;
}
bool receiveLine(
    int socket_fd,
    std::string& response
)
{
    response.clear();

    char buffer[1024];

    while (true) {

        ssize_t received = recv(
            socket_fd,
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

        auto newline =
            response.find('\n');

        if (
            newline !=
            std::string::npos
        ) {

            response.resize(
                newline + 1
            );

            return true;
        }
    }
}

}

std::optional<AppendEntriesResponse>
RaftClient::appendEntries(
    const std::string& host,
    int port,
    const AppendEntriesRequest& request
)
{
    int socket_fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (socket_fd < 0) {

        std::cerr
            << "Raft socket creation failed\n";

        return std::nullopt;
    }

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_port =
        htons(port);

    if (
        inet_pton(
            AF_INET,
            host.c_str(),
            &address.sin_addr
        ) != 1
    ) {

        std::cerr
            << "Invalid Raft peer address\n";

        close(socket_fd);

        return std::nullopt;
    }

    if (
        !connectWithTimeout(
            socket_fd,
            reinterpret_cast<sockaddr*>(
                &address
            ),
            sizeof(address)
        )
    ) {

        std::cerr
            << "Failed to connect to Raft peer\n";

        close(socket_fd);

        return std::nullopt;
    }

    if (
        !setSocketTimeouts(
            socket_fd
        )
    ) {

        std::cerr
            << "Failed to configure Raft RPC timeout\n";

        close(socket_fd);

        return std::nullopt;
    }

    std::string message =
        encodeAppendEntries(
            request
        );

    if (
        !sendAll(
            socket_fd,
            message
        )
    ) {

        std::cerr
            << "Failed to send AppendEntries\n";

        close(socket_fd);

        return std::nullopt;
    }

    std::string response;

    if (
        !receiveLine(
            socket_fd,
            response
        )
    ) {

        std::cerr
            << "Failed to receive AppendEntries response\n";

        close(socket_fd);

        return std::nullopt;
    }

    close(socket_fd);

    auto decoded =
        decodeAppendEntriesResponse(
            response
        );

    if (!decoded.has_value()) {

        std::cerr
            << "Invalid AppendEntries response: "
            << response;

        return std::nullopt;
    }

    return decoded;
}

std::optional<RequestVoteResponse>
RaftClient::requestVote(
    const std::string& host,
    int port,
    const RequestVoteRequest& request
)
{
    int socket_fd = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (socket_fd < 0) {

        std::cerr
            << "Raft socket creation failed\n";

        return std::nullopt;
    }

    sockaddr_in address{};

    address.sin_family =
        AF_INET;

    address.sin_port =
        htons(port);

    if (
        inet_pton(
            AF_INET,
            host.c_str(),
            &address.sin_addr
        ) != 1
    ) {

        std::cerr
            << "Invalid Raft peer address\n";

        close(socket_fd);

        return std::nullopt;
    }

    if (
        !connectWithTimeout(
            socket_fd,
            reinterpret_cast<sockaddr*>(
                &address
            ),
            sizeof(address)
        )
    ) {

        std::cerr
            << "Failed to connect to Raft peer\n";

        close(socket_fd);

        return std::nullopt;
    }
    if (
        !setSocketTimeouts(
            socket_fd
        )
    )
    {
        std::cerr
            << "Failed to configure Raft RPC timeout\n";

        close(socket_fd);

        return std::nullopt;
    }

    std::string message =
        encodeRequestVote(
            request
        );

    if (
        !sendAll(
            socket_fd,
            message
        )
    ) {

        std::cerr
            << "Failed to send RequestVote\n";

        close(socket_fd);

        return std::nullopt;
    }

    std::string response;

    if (
        !receiveLine(
            socket_fd,
            response
        )
    ) {

        std::cerr
            << "Failed to receive RequestVote response\n";

        close(socket_fd);

        return std::nullopt;
    }

    close(socket_fd);

    auto decoded =
        decodeRequestVoteResponse(
            response
        );

    if (!decoded.has_value()) {

        std::cerr
            << "Invalid RequestVote response: "
            << response;

        return std::nullopt;
    }

    return decoded;
}

}