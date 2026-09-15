#include "network/Server.h"
#include "commands/CommandParser.h"
#include <fcntl.h>
#include <iostream>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cerrno>
#include <unordered_map>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <netinet/tcp.h>
#include <cstring>

namespace forgedb::network {

    void Server::start()
    {
        int server_fd = socket(AF_INET, SOCK_STREAM, 0);

        if (server_fd < 0) {
            std::cerr << "Failed to create socket" << std::endl;
            return;
        }

        int flags = fcntl(server_fd, F_GETFL, 0);

        if (flags < 0 || fcntl(server_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            std::cerr << "Failed to set server socket to non-blocking" << std::endl;
            close(server_fd);
            return;
        }

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_addr.s_addr = INADDR_ANY;
        server_address.sin_port = htons(PORT);

        if (bind(server_fd,reinterpret_cast<sockaddr*>(&server_address),sizeof(server_address)) < 0) {

            std::cerr << "Failed to bind socket" << std::endl;
            close(server_fd);
            return;
        }

        if (listen(server_fd, 10) < 0) {
            std::cerr << "Failed to listen on socket" << std::endl;
            close(server_fd);
            return;
        }

        int epoll_fd = epoll_create1(0);

        if (epoll_fd < 0) {
            std::cerr << "Failed to create epoll instance" << std::endl;
            close(server_fd);
            return;
        }

        response_event_fd_ = eventfd(0, EFD_NONBLOCK);

        if (response_event_fd_ < 0) {
            std::cerr << "Failed to create eventfd" << std::endl;
            close(epoll_fd);
            close(server_fd);
            return;
        }

        epoll_event server_event{};

        server_event.events = EPOLLIN;
        server_event.data.fd = server_fd;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,server_fd,&server_event) < 0) {

            std::cerr << "Failed to add server socket to epoll"<< std::endl;

            close(response_event_fd_);
            close(epoll_fd);
            close(server_fd);
            return;
        }

        epoll_event response_event{};

        response_event.events = EPOLLIN;
        response_event.data.fd = response_event_fd_;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,response_event_fd_,&response_event) < 0) {

            std::cerr << "Failed to add response event to epoll"<< std::endl;

            close(response_event_fd_);
            close(epoll_fd);
            close(server_fd);
            return;
        }

        epoll_event events[10];

        std::cout << "ForgeDB server started on port " << PORT << std::endl;

        while (true) {

            int event_count = epoll_wait(epoll_fd,events,10,-1);

            if (event_count < 0) {
                std::cerr << "epoll_wait failed" << std::endl;
                break;
            }

            for (int i = 0; i < event_count; ++i) {
                if (events[i].data.fd == server_fd) {

                    while(true){
                        sockaddr_in client_address{};
                        socklen_t client_address_length = sizeof(client_address);

                        int client_fd = accept(server_fd,reinterpret_cast<sockaddr*>(&client_address),&client_address_length);
                        int flag = 1;
                        if (client_fd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::cerr << "Failed to accept client" << std::endl;
                            break;
                        }
                        if (setsockopt(
                                client_fd,
                                IPPROTO_TCP,
                                TCP_NODELAY,
                                &flag,
                                sizeof(flag)) < 0) {

                            std::cerr << "Failed to enable TCP_NODELAY for client\n";
                        }

                        int client_flags = fcntl(client_fd, F_GETFL, 0);

                        if (client_flags < 0 || fcntl(client_fd, F_SETFL, client_flags | O_NONBLOCK) < 0) {
                            std::cerr << "Failed to set client socket to non-blocking" << std::endl;
                            close(client_fd);
                            continue;
                        }

                        epoll_event client_event{};

                        client_event.events = EPOLLIN;
                        client_event.data.fd = client_fd;

                        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_event) < 0) {
                            std::cerr << "Failed to add client socket to epoll" << std::endl;
                            close(client_fd);
                            continue;
                        }
                        active_clients_.insert(client_fd);
                        uint64_t generation = next_generation_++;
                        clients_.emplace(client_fd, ClientState{client_fd, generation});
                    }
                }
                else if (events[i].data.fd == response_event_fd_) {

                    uint64_t value;

                    if (read(response_event_fd_, &value, sizeof(value)) < 0) {
                        std::cerr << "eventfd read failed: "
                                  << strerror(errno) << std::endl;
                    }

                    while (true) {

                        ClientResponse response;

                        {
                            std::lock_guard<std::mutex> lock(responseMutex_);

                            if (responseQueue_.empty()) {
                                break;
                            }

                            response = std::move(responseQueue_.front());
                            responseQueue_.pop();
                        }

                        auto it = clients_.find(response.client_fd);

                        if (it != clients_.end() && it->second.generation == response.generation && it->second.active) {
                            it->second.output_buffer += response.response;
                            enableWrite(epoll_fd, response.client_fd);
                        }
                    }
                }
                else {
                    int client_fd = events[i].data.fd;

                    if (events[i].events & EPOLLOUT) {
                        handleWrite(epoll_fd, client_fd);
                    }

                    if (!(events[i].events & EPOLLIN)) {
                        continue;
                    }

                    while(true){
                        char buffer[1024];

                        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);

                        if (bytes_received > 0) {

                            buffer[bytes_received] = '\0';

                            clients_[client_fd].receive_buffer += buffer;

                            size_t newline_pos;

                            while ((newline_pos = clients_[client_fd].receive_buffer.find('\n')) != std::string::npos) {
                                std::string command = clients_[client_fd].receive_buffer.substr(0, newline_pos);
                                clients_[client_fd].receive_buffer.erase(0, newline_pos + 1);
                                enqueueCommand(client_fd, command);
                            }
                        }
                        else if (bytes_received == 0) {

                            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);

                            close(client_fd);

                            active_clients_.erase(client_fd);
                            clients_.erase(client_fd);

                            break;
                        }
                        else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::cerr << "recv failed: "<< errno<< " - "<< strerror(errno)<< std::endl;

                            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);

                            close(client_fd);

                            active_clients_.erase(client_fd);
                            clients_.erase(client_fd);

                            break;

                        }
                    }
                }
            }
        }

        close(server_fd);
    }

    ClientResponse Server::processCommand(int client_fd,const std::string& command)
    {
        forgedb::commands::CommandParser parser;

        auto parsed = parser.parse(command);

        std::string response = commandHandler_.execute(parsed);

        return ClientResponse{
            client_fd,
            0, // generation will be set later
            response
        };
    }
    void Server::handleWrite(int epoll_fd, int client_fd)
    {
        auto it = clients_.find(client_fd);

        if (it == clients_.end()) {
            return;
        }

        std::string& output = it->second.output_buffer;
        if (output.empty()) {
            return;
        }

        ssize_t bytes_sent = send(client_fd,output.data(),output.size(),0);

        if (bytes_sent > 0) {
            output.erase(0, bytes_sent);
            if (output.empty()) {

                // No more data to send.
                // Stop watching this socket for EPOLLOUT.

                epoll_event client_event{};

                client_event.events = EPOLLIN;
                client_event.data.fd = client_fd;

                if (epoll_ctl(epoll_fd,EPOLL_CTL_MOD,client_fd,&client_event) < 0) {
                    std::cerr << "Failed to disable EPOLLOUT"<< std::endl;
                }
            }
        }
        else if (bytes_sent < 0) {

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }

            std::cerr << "send failed: "<< errno<< " - "<< strerror(errno)<< std::endl;
        }
    }
    void Server::enableWrite(int epoll_fd, int client_fd)
    {
        epoll_event client_event{};

        client_event.events = EPOLLIN | EPOLLOUT;
        client_event.data.fd = client_fd;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_MOD,client_fd,&client_event) < 0) {
            std::cerr << "Failed to enable EPOLLOUT for client"<< std::endl;
        }
    }
    void Server::enqueueCommand(int client_fd,const std::string& command)
    {
        bool should_process = false;

        {
            std::lock_guard<std::mutex> lock(responseMutex_);

            auto it = clients_.find(client_fd);

            if (it == clients_.end()) {
                return;
            }

            ClientState& client = it->second;

            constexpr std::size_t MAX_QUEUE_SIZE = 1000;

            if (client.command_queue.size() >= MAX_QUEUE_SIZE) {
                responseQueue_.push(
                    ClientResponse{
                        client_fd,
                        client.generation,
                        "ERR server busy\n"
                    }
                );

                uint64_t value = 1;
                write(response_event_fd_, &value, sizeof(value));
                return;
            }
            client.command_queue.push_back(command);

            if (!client.processing) {
                client.processing = true;
                should_process = true;
            }
        }

        if (should_process) {
            processNextCommand(client_fd);
        }
    }
    void Server::processNextCommand(int client_fd)
    {
        threadPool_.submit([this, client_fd]() {

            while (true) {

                std::string command;
                uint64_t generation;

                {
                    std::lock_guard<std::mutex> lock(responseMutex_);

                    auto it = clients_.find(client_fd);

                    if (it == clients_.end()) {
                        return;
                    }

                    ClientState& client = it->second;

                    if (client.command_queue.empty()) {
                        client.processing = false;
                        return;
                    }

                    generation = client.generation;

                    command = std::move(client.command_queue.front());
                    client.command_queue.pop_front();
                }

                ClientResponse result = processCommand(client_fd, command);

                result.generation = generation;

                {
                    std::lock_guard<std::mutex> lock(responseMutex_);
                    responseQueue_.push(std::move(result));
                }

                uint64_t value = 1;
                if (write(response_event_fd_, &value, sizeof(value)) < 0) {
                    std::cerr << "eventfd write failed: "
                              << strerror(errno) << std::endl;
                }
            }
        });
    }
}
