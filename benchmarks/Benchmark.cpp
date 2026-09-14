#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

int main()
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0) {
        std::cerr << "Failed to create socket\n";
        return 1;
    }

    sockaddr_in server_addr{};

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(6379);

    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(
            sock,
            reinterpret_cast<sockaddr*>(&server_addr),
            sizeof(server_addr)) < 0) {

        std::cerr << "Failed to connect to ForgeDB\n";
        close(sock);
        return 1;
    }

    const char* command = "SET benchmark 123\n";

    auto start = std::chrono::steady_clock::now();

    send(sock, command, strlen(command), 0);

    char buffer[1024];

    ssize_t bytes_received =
        recv(sock, buffer, sizeof(buffer) - 1, 0);

    auto end = std::chrono::steady_clock::now();

    if (bytes_received <= 0) {
        std::cerr << "Failed to receive response\n";
        close(sock);
        return 1;
    }

    buffer[bytes_received] = '\0';

    auto latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            end - start
        );

    std::cout << "Response: " << buffer;
    std::cout << "Latency: "
              << latency.count()
              << " us\n";

    close(sock);

    return 0;
}