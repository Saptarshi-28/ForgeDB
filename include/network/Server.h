#pragma once
#include "storage/KeyValueStore.h"
#include "commands/CommandHandler.h"
#include <string>

namespace forgedb::network {

	class Server {
		public:
    		void start();

		private:
    		static constexpr int PORT=6379;
				
			void handleClient(int client_fd);
			void processCommand(int client_fd,const std::string& command);

			forgedb::storage::KeyValueStore store_{"forge.wal"};
			forgedb::commands::CommandHandler commandHandler_{store_};
	};
}
