#pragma once

#include "commands/Command.h"

#include <string>

namespace forgedb::commands {

	class CommandParser {
		public:
    			Command parse(const std::string& input);
		};

}
