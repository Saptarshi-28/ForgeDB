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

        epoll_event server_event{};

        server_event.events = EPOLLIN;
        server_event.data.fd = server_fd;

        if (epoll_ctl(epoll_fd,EPOLL_CTL_ADD,server_fd,&server_event) < 0) {

            std::cerr << "Failed to add server socket to epoll" << std::endl;

            close(epoll_fd);
            close(server_fd);
            return;
        }
        epoll_event events[10];

        std::cout << "ForgeDB server started on port " << PORT << std::endl;

        std::unordered_map<int, std::string> receive_buffers;

        while (true) {

            //sockaddr_in client_address{};
            //socklen_t client_address_length = sizeof(client_address);
            //int client_fd = accept(
            //    server_fd,
            //    reinterpret_cast<sockaddr*>(&client_address),
            //    &client_address_length
            //);
            //if (client_fd < 0) {
            //    if (errno == EAGAIN || errno == EWOULDBLOCK) {
            //        continue;
            //    }
            //    std::cerr << "Failed to accept client" << std::endl;
            //    continue;
            //}
            //std::cout << "Client connected!" << std::endl;
    		//std::thread client_thread(&Server::handleClient, this, client_fd);
    		//client_thread.detach();
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
                    
                        if (client_fd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            std::cerr << "Failed to accept client" << std::endl;
                            break;
                        }
                    
                        std::cout << "Client connected!" << std::endl;

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
                    }
                }
                else {
                    int client_fd = events[i].data.fd;
                    while(true){
                        char buffer[1024];
                    
                        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
                    
                        if (bytes_received > 0) {
                        
                            buffer[bytes_received] = '\0';
                        
                            receive_buffers[client_fd] += buffer;
                        
                            size_t newline_pos;
                        
                            while ((newline_pos = receive_buffers[client_fd].find('\n')) != std::string::npos) {
                                
                                std::string command = receive_buffers[client_fd].substr(0,newline_pos);
                                
                                receive_buffers[client_fd].erase(0,newline_pos + 1);
                            
                                processCommand(client_fd, command);
                            }
                        }
                        else if (bytes_received == 0) {
                        
                            std::cout << "Client disconnected" << std::endl;
                        
                            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);
                        
                            close(client_fd);
                        
                            receive_buffers.erase(client_fd);

                            break;
                        }
                        else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::cerr << "recv failed: "<< errno<< " - "<< strerror(errno)<< std::endl;
                            
                            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,client_fd,nullptr);
                            
                            close(client_fd);
                            receive_buffers.erase(client_fd);

                            break;
                            
                        }
                    }
                }
            }
        }
        close(server_fd);
    }    



    void Server::handleClient(int client_fd)
    {
        std::string receive_buffer;

        while (true) {

            char buffer[1024];

            ssize_t bytes_received =
                recv(client_fd, buffer, sizeof(buffer) - 1, 0);

            if (bytes_received <= 0) {
                std::cout << "Client disconnected" << std::endl;
                break;
            }

            buffer[bytes_received] = '\0';

            receive_buffer += buffer;

            size_t newline_pos;

            while ((newline_pos = receive_buffer.find('\n')) != std::string::npos) {

                std::string command =
                    receive_buffer.substr(0, newline_pos);

                receive_buffer.erase(
                    0,
                    newline_pos + 1
                );

                processCommand(client_fd, command);
            }
        }

        close(client_fd);
    }

    void Server::processCommand(int client_fd,const std::string& command)
    {
        forgedb::commands::CommandParser parser;

        auto parsed = parser.parse(command);

        std::string response = commandHandler_.execute(parsed);

        send(client_fd,response.c_str(),response.size(),0);
    }
}