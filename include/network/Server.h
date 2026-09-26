#pragma once
#include "storage/KeyValueStore.h"
#include "commands/CommandHandler.h"
#include <string>
#include <mutex>
#include <queue>
#include "concurrency/ThreadPool.h"
#include <unordered_map>
#include "network/ClientState.h"
#include <unordered_set>
#include <cstdint>

namespace forgedb::network {
	
	struct ClientResponse {
    	int client_fd;
		uint64_t generation;
    	std::string response;
	};

	class Server {
		public:
    		void start();

		private:
    		static constexpr int PORT=6379;
			int response_event_fd_ = -1;
			uint64_t next_generation_ = 1;

			ClientResponse processCommand(int client_fd,const std::string& command);
			void handleWrite(int epoll_fd, int client_fd);
			void enableWrite(int epoll_fd, int client_fd);
			void enqueueCommand(int client_fd, const std::string& command);
			void processNextCommand(int client_fd);

			forgedb::storage::KeyValueStore store_{"forge.wal"};
			forgedb::commands::CommandHandler commandHandler_{store_};
			forgedb::concurrency::ThreadPool threadPool_{8};

			std::unordered_map<int, std::string> output_buffers_;
			std::queue<ClientResponse> responseQueue_;
			std::mutex responseMutex_;
	
			std::unordered_set<int> active_clients_;
			std::unordered_map<int, ClientState> clients_;
	};
}
