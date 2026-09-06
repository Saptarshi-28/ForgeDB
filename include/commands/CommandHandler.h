#pragma once

#include "commands/Command.h"
#include "storage/KeyValueStore.h"

#include <string>

namespace forgedb::commands {

class CommandHandler {
	public:
    	CommandHandler(forgedb::storage::KeyValueStore& store);

    	std::string execute(const Command& command);

	private:
    	forgedb::storage::KeyValueStore& store_;
};

}