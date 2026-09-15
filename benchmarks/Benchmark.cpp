#include <arpa/inet.h>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

constexpr int NUM_CLIENTS = 8;
constexpr int OPERATIONS_PER_CLIENT = 250;
constexpr int PIPELINE_SIZE = 16;

void runClient(int client_id)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0) {
        std::cerr << "Failed to create socket\n";
        return;
    }

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(6379);

    inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr);

    if (connect(
            sock,
            reinterpret_cast<sockaddr*>(&server_address),
            sizeof(server_address)) < 0) {

        std::cerr << "Failed to connect\n";
        close(sock);
        return;
    }

    char buffer[4096];

    int operations_completed = 0;

    // IMPORTANT:
    // This must live across pipeline batches.
    std::string response_buffer;

    while (operations_completed < OPERATIONS_PER_CLIENT) {

        int batch_size = std::min(
            PIPELINE_SIZE,
            OPERATIONS_PER_CLIENT - operations_completed
        );

        // Send a batch of commands without waiting for responses.
        for (int i = 0; i < batch_size; ++i) {

            int operation_number =
                operations_completed + i;

            std::string command =
                "SET client" +
                std::to_string(client_id) +
                "_key" +
                std::to_string(operation_number) +
                " value\n";

            ssize_t sent = send(
                sock,
                command.c_str(),
                command.size(),
                0
            );

            if (sent < 0) {
                std::cerr << "Send failed\n";
                close(sock);
                return;
            }
        }

        // Now read all responses belonging to this batch.
        int responses_received = 0;

        while (responses_received < batch_size) {

            // First consume any complete responses that were
            // already received from a previous recv().
            size_t position;

            while ((position = response_buffer.find('\n'))
                   != std::string::npos) {

                response_buffer.erase(0, position + 1);
                responses_received++;

                if (responses_received == batch_size) {
                    break;
                }
            }

            if (responses_received == batch_size) {
                break;
            }

            ssize_t received = recv(
                sock,
                buffer,
                sizeof(buffer),
                0
            );

            if (received <= 0) {
                std::cerr
                    << "Connection closed or recv failed\n";
                close(sock);
                return;
            }

            response_buffer.append(buffer, received);
        }

        operations_completed += batch_size;
    }

    close(sock);
}

int main()
{
    constexpr int TOTAL_OPERATIONS =
        NUM_CLIENTS * OPERATIONS_PER_CLIENT;

    std::vector<std::thread> clients;

    clients.reserve(NUM_CLIENTS);

    auto benchmark_start =
        std::chrono::steady_clock::now();

    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back(runClient, i);
    }

    for (auto& client : clients) {
        client.join();
    }

    auto benchmark_end =
        std::chrono::steady_clock::now();

    auto total_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            benchmark_end - benchmark_start
        );

    double total_seconds =
        total_time.count() / 1'000'000.0;

    double throughput =
        TOTAL_OPERATIONS / total_seconds;

    std::cout << "\nForgeDB Pipelined Benchmark\n";
    std::cout << "---------------------------\n";
    std::cout << "Clients:           "
              << NUM_CLIENTS << "\n";

    std::cout << "Operations/client: "
              << OPERATIONS_PER_CLIENT << "\n";

    std::cout << "Pipeline size:     "
              << PIPELINE_SIZE << "\n";

    std::cout << "Total operations:  "
              << TOTAL_OPERATIONS << "\n";

    std::cout << "Total time:        "
              << total_time.count()
              << " us\n";

    std::cout << "Throughput:        "
              << throughput
              << " ops/sec\n";

    return 0;
}
