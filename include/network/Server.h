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
#include "replication/ReplicaClient.h"
#include "replication/ReplicationEntry.h"
#include "replication/ReplicationLog.h"

namespace forgedb::network {

	struct ClientResponse {
    	int client_fd;
		uint64_t generation;
    	std::string response;
	};

	class Server {
		public:
			Server(
			    int port = 6379,
			    const std::string& replica_host = "",
			    int replica_port = 0
			);
    		void start();

		private:
    		int port_;
			int response_event_fd_ = -1;
			uint64_t next_generation_ = 1;

			ClientResponse processCommand(int client_fd,const std::string& command);
			void handleWrite(int epoll_fd, int client_fd);
			void enableWrite(int epoll_fd, int client_fd);
			void enqueueCommand(int client_fd, const std::string& command);
			void processNextCommand(int client_fd);
			bool replicateWithReconnect(const std::string& command);
			ClientResponse processReplicationCommand(int client_fd,const std::string& command);
			bool loadReplicationState();
			bool persistReplicationState(uint64_t sequence);
			bool replayReplicationLog();

			forgedb::storage::KeyValueStore store_{"forge.wal"};
			forgedb::commands::CommandHandler commandHandler_{store_};
			forgedb::concurrency::ThreadPool threadPool_{8};

			std::unordered_map<int, std::string> output_buffers_;
			std::queue<ClientResponse> responseQueue_;
			std::mutex responseMutex_;

			std::mutex replicaApplyMutex_;
			uint64_t last_applied_replication_sequence_ = 0;

			std::unordered_set<int> active_clients_;
			std::unordered_map<int, ClientState> clients_;

			std::string replica_host_;
			int replica_port_ = 0;
			bool replication_enabled_ = false;

			forgedb::replication::ReplicaClient replica_client_;
			std::mutex replicationMutex_;

			forgedb::replication::ReplicationLog replication_log_{"replication.log"};

			uint64_t next_replication_to_send_ = 1;
	};
}
