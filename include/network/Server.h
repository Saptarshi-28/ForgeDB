#pragma once
#include "storage/KeyValueStore.h"
#include "commands/CommandHandler.h"

namespace forgedb::network {

	class Server {
		public:
    			void start();

		private:
    			static constexpr int PORT=6379;
				forgedb::storage::KeyValueStore store_{"forge.wal"};
				forgedb::commands::CommandHandler commandHandler_{store_};
	};
}
