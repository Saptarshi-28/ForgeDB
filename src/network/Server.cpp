#include "commands/CommandParser.h"
#include "network/Server.h"
#include <arpa/inet.h>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

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

	forgedb::commands::CommandParser parser;
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
		
		std::string receive_buffer;
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
			receive_buffer+=buffer;
			
			//PARSING COUPLED COMMANDS
			size_t newline_pos;
			while((newline_pos=receive_buffer.find('\n'))!=std::string::npos){
			
				std::string command=receive_buffer.substr(0,newline_pos);
				receive_buffer.erase(0,newline_pos+1);
				auto parsed = parser.parse(command);
			
				if (parsed.type == forgedb::commands::CommandType::SET) {

 					store_.set(parsed.key, parsed.value);

    					const char* response = "OK\n";

    					send(client_fd,response,std::strlen(response),0);
				}
				else if (parsed.type == forgedb::commands::CommandType::GET) {

    					std::string result = store_.get(parsed.key);

    					if (result.empty()) result = "(nil)\n";
    					else result += "\n";

    					send(client_fd,result.c_str(),result.size(),0);
				}
				else if (parsed.type == forgedb::commands::CommandType::DELETE) {

    					bool removed = store_.remove(parsed.key);

    					const char* response = removed ? "OK\n" : "(nil)\n";

    					send(client_fd,response,std::strlen(response),0);
				}
				else {

    					const char* response = "ERR unknown command\n";

    					send(client_fd,response,std::strlen(response),0);
				}
			}
		}
		//CLOSE
		close(client_fd);

	}
    }
}
