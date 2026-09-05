#include "network/Server.h"
#include <arpa/inet.h>
#include <cstring>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <string>

namespace forgedb::network {

void Server::start() {
	//CREATE SCOKET
    	int server_fd=socket(AF_INET,SOCK_STREAM,0);

    	if(server_fd==-1){
		std::cerr<<"Socket creation Fail ho gaya\n";
		return;
	}
	//CONFIGURE
	sockaddr_in server_address{};

	server_address.sin_family=AF_INET;
	server_address.sin_addr.s_addr=INADDR_ANY;
	server_address.sin_port=htons(PORT);

	//BIND
	if(bind(
		server_fd,
		reinterpret_cast<sockaddr*>(&server_address),
		sizeof(server_address))==-1){

	   std::cerr<<"Failed to bind Socket\n";
	   close(server_fd);
	   return;
	}
	//LISTEN
	if(listen(server_fd,10)==-1){
		std::cerr<<"Failed to listen\n";
		close(server_fd);
		return;
	}
	std::cout<<"ForgeDB server LISTENING on port "<<PORT<<std::endl;

	//ACCEPT CLIENTS
	while(true){
		sockaddr_in client_address{};
		socklen_t client_length=sizeof(client_address);

		int client_fd=accept(
			server_fd,
			reinterpret_cast<sockaddr*>(&client_address),
			&client_length
		);

		if(client_fd==-1){
			std::cerr<<"Failed TO accept client\n";
			close(server_fd);
			return;
		}
		std::cout<<"Client connected!"<<std::endl;

		//RECIEVE
		while(true){
			char buffer[1024];

			ssize_t bytes_recieved=recv(client_fd,buffer,sizeof(buffer)-1,0);

			//CLIENT disconnected
			if(bytes_recieved<=0){
				std::cout<<"Client disconnected"<<std::endl;
				break;
			}
			buffer[bytes_recieved]='\0';

			std::string command(buffer);

			std::istringstream stream(command);

			std::string operation;
			std::string key;
			std::string value;

			stream >> operation;

			if (operation == "SET") {

    				stream >> key;
    				stream >> value;

    				store_.set(key, value);

    				const char* response = "OK\n";

    				send(client_fd,response,std::strlen(response),0);

			}
			else if (operation == "GET") {

    				stream >> key;

    				std::string result = store_.get(key);

    				if (result.empty()) result = "(nil)\n";
				else result += "\n";

    				send(client_fd,result.c_str(),result.size(),0);

			}
			else if (operation == "DELETE") {

    				stream >> key;

    				bool removed = store_.remove(key);

    				const char* response = removed ? "OK\n" : "(nil)\n";

    				send(client_fd,response,std::strlen(response),0);

			}
			else {

    				const char* response = "ERR unknown command\n";

    				send(client_fd,response,std::strlen(response),0);
			}

		}
		//CLOSE
		close(client_fd);

	}
    }
}
