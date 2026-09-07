#include "network/Server.h"
#include "commands/CommandParser.h"

#include <iostream>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

namespace forgedb::network {

void Server::start()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
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

    std::cout << "ForgeDB server started on port " << PORT << std::endl;

    while (true) {

        sockaddr_in client_address{};
        socklen_t client_address_length = sizeof(client_address);

        int client_fd = accept(
            server_fd,
            reinterpret_cast<sockaddr*>(&client_address),
            &client_address_length
        );

        if (client_fd < 0) {
            std::cerr << "Failed to accept client" << std::endl;
            continue;
        }

        std::cout << "Client connected!" << std::endl;

        //handleClient(client_fd);
		std::thread client_thread(&Server::handleClient, this, client_fd);
		client_thread.detach();
    }

    close(server_fd);
}


void Server::handleClient(int client_fd)
{
    std::string receive_buffer;
    forgedb::commands::CommandParser parser;

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

        while ((newline_pos = receive_buffer.find('\n'))
               != std::string::npos) {

            std::string command =
                receive_buffer.substr(0, newline_pos);

            receive_buffer.erase(
                0,
                newline_pos + 1
            );

            auto parsed = parser.parse(command);

            std::string response =
                commandHandler_.execute(parsed);

            send(client_fd,response.c_str(),response.size(),0);
        }
    }

    close(client_fd);
}

}