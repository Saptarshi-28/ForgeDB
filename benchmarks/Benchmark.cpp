#include <arpa/inet.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <functional>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <stdexcept>

constexpr int NUM_CLIENTS = 8;
constexpr int OPERATIONS_PER_CLIENT = 250;
constexpr int PIPELINE_SIZE = 16;
enum class Workload {
    WRITE,
    READ,
    MIXED
};
struct LatencyResults {
    std::vector<long long> all;
    std::vector<long long> reads;
    std::vector<long long> writes;
};

bool sendAll(
    int sock,
    const std::string& command
)
{
    std::size_t total_sent = 0;

    while (total_sent < command.size()) {

        ssize_t sent = send(
            sock,
            command.data() + total_sent,
            command.size() - total_sent,
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

int connectToServer()
{
    int sock = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (sock < 0) {
        return -1;
    }

    sockaddr_in server_address{};

    server_address.sin_family =
        AF_INET;

    server_address.sin_port =
        htons(6379);

    inet_pton(
        AF_INET,
        "127.0.0.1",
        &server_address.sin_addr
    );

    if (connect(
            sock,
            reinterpret_cast<sockaddr*>(
                &server_address
            ),
            sizeof(server_address)
        ) < 0) {

        close(sock);
        return -1;
    }

    return sock;
}

void preloadClient(
    int client_id
)
{
    int sock = connectToServer();

    if (sock < 0) {
        std::cerr
            << "Preload connection failed\n";
        return;
    }

    char buffer[4096];

    std::string response_buffer;

    int operations_completed = 0;

    while (
        operations_completed <
        OPERATIONS_PER_CLIENT
    ) {

        int batch_size = std::min(
            PIPELINE_SIZE,
            OPERATIONS_PER_CLIENT -
                operations_completed
        );

        for (int i = 0;
             i < batch_size;
             ++i) {

            int operation_number =
                operations_completed + i;

            std::string command =
                "SET bench_client" +
                std::to_string(client_id) +
                "_key" +
                std::to_string(operation_number) +
                " value\n";

            if (!sendAll(sock, command)) {
                std::cerr
                    << "Preload send failed\n";

                close(sock);
                return;
            }
        }

        int responses_received = 0;

        while (
            responses_received <
            batch_size
        ) {

            std::size_t position;

            while (
                (position =
                    response_buffer.find('\n'))
                != std::string::npos
            ) {

                response_buffer.erase(
                    0,
                    position + 1
                );

                ++responses_received;

                if (
                    responses_received ==
                    batch_size
                ) {
                    break;
                }
            }

            if (
                responses_received ==
                batch_size
            ) {
                break;
            }

            ssize_t received = recv(
                sock,
                buffer,
                sizeof(buffer),
                0
            );

            if (received < 0) {

                if (errno == EINTR) {
                    continue;
                }

                std::cerr
                    << "Preload recv failed\n";

                close(sock);
                return;
            }

            if (received == 0) {
                std::cerr
                    << "Preload connection closed\n";

                close(sock);
                return;
            }

            response_buffer.append(
                buffer,
                static_cast<std::size_t>(
                    received
                )
            );
        }

        operations_completed +=
            batch_size;
    }

    close(sock);
}

void runClient(
    int client_id,
    Workload workload,
    LatencyResults& results
)
{
    int sock = connectToServer();

    if (sock < 0) {
        std::cerr
            << "Benchmark connection failed\n";
        return;
    }

    char buffer[4096];

    std::string response_buffer;

    int operations_completed = 0;

    results.all.reserve(
        OPERATIONS_PER_CLIENT
    );

    results.reads.reserve(
        OPERATIONS_PER_CLIENT
    );

    results.writes.reserve(
        OPERATIONS_PER_CLIENT
    );

    while (
        operations_completed <
        OPERATIONS_PER_CLIENT
    ) {

        int batch_size = std::min(
            PIPELINE_SIZE,
            OPERATIONS_PER_CLIENT -
                operations_completed
        );

        std::vector<
            std::chrono::steady_clock::time_point
        > send_times;

        send_times.reserve(
            batch_size
        );
        std::vector<bool> batch_is_write;

        batch_is_write.reserve(
            batch_size
        );

        for (int i = 0;
             i < batch_size;
             ++i) {
            bool is_write = false;
            int operation_number =
                operations_completed + i;

            std::string key =
                "bench_client" +
                std::to_string(client_id) +
                "_key" +
                std::to_string(operation_number);

            std::string command;

            if (workload == Workload::WRITE) {

                is_write = true;

                command =
                    "SET " +
                    key +
                    " value\n";
            }
            else if (workload == Workload::READ) {

                is_write = false;

                command =
                    "GET " +
                    key +
                    "\n";
            }
            else {

                // MIXED:
                // 80% GET / 20% SET
                if (operation_number % 5 == 0) {

                    is_write = true;

                    command =
                        "SET " +
                        key +
                        " mixed_value\n";
                }
                else {

                    is_write = false;

                    command =
                        "GET " +
                        key +
                        "\n";
                }
            }

            auto request_start =
                std::chrono::steady_clock::now();

            if (!sendAll(sock, command)) {

                std::cerr
                    << "Benchmark send failed\n";

                close(sock);
                return;
            }

            send_times.push_back(
                request_start
            );
            batch_is_write.push_back(
                is_write
            );
        }

        int responses_received = 0;

        while (
            responses_received <
            batch_size
        ) {

            std::size_t position;

            while (
                (position =
                    response_buffer.find('\n'))
                != std::string::npos
            ) {

                auto response_time =
                    std::chrono::
                        steady_clock::now();

                auto latency =
                    std::chrono::
                        duration_cast<
                            std::chrono::
                                microseconds
                        >(
                            response_time -
                            send_times[
                                responses_received
                            ]
                        );

                results.all.push_back(
                    latency.count()
                );

                if (
                    batch_is_write[
                        responses_received
                    ]
                ) {
                    results.writes.push_back(
                        latency.count()
                    );
                }
                else {
                    results.reads.push_back(
                        latency.count()
                    );
                }

                response_buffer.erase(
                    0,
                    position + 1
                );

                ++responses_received;

                if (
                    responses_received ==
                    batch_size
                ) {
                    break;
                }
            }

            if (
                responses_received ==
                batch_size
            ) {
                break;
            }

            ssize_t received = recv(
                sock,
                buffer,
                sizeof(buffer),
                0
            );

            if (received < 0) {

                if (errno == EINTR) {
                    continue;
                }

                std::cerr
                    << "Benchmark recv failed\n";

                close(sock);
                return;
            }

            if (received == 0) {

                std::cerr
                    << "Benchmark connection closed\n";

                close(sock);
                return;
            }

            response_buffer.append(
                buffer,
                static_cast<std::size_t>(
                    received
                )
            );
        }

        operations_completed +=
            batch_size;
    }

    close(sock);
}

long long percentile(
    const std::vector<long long>&
        sorted_latencies,
    double p
)
{
    if (sorted_latencies.empty()) {
        return 0;
    }

    std::size_t index =
        static_cast<std::size_t>(
            std::ceil(
                p *
                sorted_latencies.size()
            )
        ) - 1;

    index = std::min(
        index,
        sorted_latencies.size() - 1
    );

    return sorted_latencies[index];
}
Workload parseWorkload(
    const std::string& value
)
{
    if (value == "write") {
        return Workload::WRITE;
    }

    if (value == "read") {
        return Workload::READ;
    }

    if (value == "mixed") {
        return Workload::MIXED;
    }

    throw std::runtime_error(
        "Unknown workload: " + value
    );
}


int main(int argc, char* argv[])
{
    if (argc != 2) {
        std::cerr
            << "Usage: "
            << argv[0]
            << " <write|read|mixed>\n";

        return 1;
    }

    Workload workload;

    try {
        workload =
            parseWorkload(argv[1]);
    }
    catch (const std::exception& e) {

        std::cerr
            << e.what()
            << "\n";

        std::cerr
            << "Valid workloads: "
            << "write, read, mixed\n";

        return 1;
    }

    constexpr int TOTAL_OPERATIONS =
        NUM_CLIENTS *
        OPERATIONS_PER_CLIENT;

    if (
    workload == Workload::READ ||
    workload == Workload::MIXED
    ){
    std::cout
        << "Preloading "
        << TOTAL_OPERATIONS
        << " keys...\n";

    std::vector<std::thread>
        preload_clients;

    preload_clients.reserve(
        NUM_CLIENTS
    );

    for (int i = 0;
         i < NUM_CLIENTS;
         ++i) {

        preload_clients.emplace_back(
            preloadClient,
            i
        );
    }

    for (auto& client :
         preload_clients) {

        client.join();
    }

    std::cout
        << "Preload complete.\n";
    }
    std::vector<LatencyResults>
    client_results(
        NUM_CLIENTS
    );

    std::vector<std::thread>
        benchmark_clients;

    benchmark_clients.reserve(
        NUM_CLIENTS
    );

    // Timing begins ONLY for GET workload.
    auto benchmark_start =
        std::chrono::steady_clock::now();

    for (int i = 0;
         i < NUM_CLIENTS;
         ++i) {

        benchmark_clients.emplace_back(
            runClient,
            i,
            workload,
            std::ref(
                client_results[i]
            )
        );
    }

    for (auto& client :
        benchmark_clients) {

        client.join();
    }

    auto benchmark_end =
        std::chrono::steady_clock::now();

    std::vector<long long> latencies;
    std::vector<long long> read_latencies;
    std::vector<long long> write_latencies;

    latencies.reserve(
        TOTAL_OPERATIONS
    );

    read_latencies.reserve(
        TOTAL_OPERATIONS
    );

    write_latencies.reserve(
        TOTAL_OPERATIONS
    );

    for (
        const auto& result :
        client_results
    ) {

        latencies.insert(
            latencies.end(),
            result.all.begin(),
            result.all.end()
        );

        read_latencies.insert(
            read_latencies.end(),
            result.reads.begin(),
            result.reads.end()
        );

        write_latencies.insert(
            write_latencies.end(),
            result.writes.begin(),
            result.writes.end()
        );
    }

    std::sort(
        latencies.begin(),
        latencies.end()
    );
    std::sort(
        read_latencies.begin(),
        read_latencies.end()
    );

    std::sort(
        write_latencies.begin(),
        write_latencies.end()
    );

    auto total_time =
        std::chrono::
            duration_cast<
                std::chrono::microseconds
            >(
                benchmark_end -
                benchmark_start
            );

    double total_seconds =
        total_time.count() /
        1'000'000.0;

    std::size_t completed_operations =
        latencies.size();

    double throughput =
        completed_operations /
        total_seconds;

    long long p50 =
        percentile(
            latencies,
            0.50
        );

    long long p95 =
        percentile(
            latencies,
            0.95
        );

    long long p99 =
        percentile(
            latencies,
            0.99
        );

    long long read_p50 =
        percentile(
            read_latencies,
            0.50
        );

    long long read_p95 =
        percentile(
            read_latencies,
            0.95
        );

    long long read_p99 =
        percentile(
            read_latencies,
            0.99
        );

    long long write_p50 =
        percentile(
            write_latencies,
            0.50
        );

    long long write_p95 =
        percentile(
            write_latencies,
            0.95
        );

    long long write_p99 =
        percentile(
            write_latencies,
            0.99
        );

    std::cout
    << "\nForgeDB ";

    if (workload == Workload::WRITE) {
        std::cout << "WRITE";
    }
    else if (workload == Workload::READ) {
        std::cout << "READ";
    }
    else {
        std::cout << "MIXED";
    }

    std::cout
        << " Benchmark\n";

    std::cout
        << "-----------------------\n";

    if (workload == Workload::MIXED) {
        std::cout
            << "Workload:             "
            << "80% GET / 20% SET\n";
    }

    std::cout
        << "Clients:              "
        << NUM_CLIENTS
        << "\n";

    std::cout
        << "Operations/client:    "
        << OPERATIONS_PER_CLIENT
        << "\n";

    std::cout
        << "Pipeline size:        "
        << PIPELINE_SIZE
        << "\n";

    std::cout
        << "Requested operations: "
        << TOTAL_OPERATIONS
        << "\n";

    std::cout
        << "Completed operations: "
        << completed_operations
        << "\n";

    std::cout
        << "Total time:           "
        << total_time.count()
        << " us\n";

    std::cout
        << "Throughput:           "
        << throughput
        << " ops/sec\n";

    std::cout
        << "P50 latency:          "
        << p50
        << " us\n";

    std::cout
        << "P95 latency:          "
        << p95
        << " us\n";

    std::cout
        << "P99 latency:          "
        << p99
        << " us\n";

    if (workload == Workload::MIXED) {

        std::cout
            << "\nGET statistics\n";

        std::cout
            << "--------------\n";

        std::cout
            << "GET operations:       "
            << read_latencies.size()
            << "\n";

        std::cout
            << "GET P50 latency:      "
            << read_p50
            << " us\n";

        std::cout
            << "GET P95 latency:      "
            << read_p95
            << " us\n";

        std::cout
            << "GET P99 latency:      "
            << read_p99
            << " us\n";

        std::cout
            << "\nSET statistics\n";

        std::cout
            << "--------------\n";

        std::cout
            << "SET operations:       "
            << write_latencies.size()
            << "\n";

        std::cout
            << "SET P50 latency:      "
            << write_p50
            << " us\n";

        std::cout
            << "SET P95 latency:      "
            << write_p95
            << " us\n";

        std::cout
            << "SET P99 latency:      "
            << write_p99
            << " us\n";
    }
    return 0;
}