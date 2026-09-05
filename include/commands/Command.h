#pragma once

#include <string>

namespace forgedb::commands {

	enum class CommandType {SET,GET,DELETE,UNKNOWN};

	struct Command {
    		CommandType type = CommandType::UNKNOWN;

    		std::string key;
    		std::string value;
	};

}
