#include "commands/CommandParser.h"

#include <sstream>

namespace forgedb::commands {

	Command CommandParser::parse(const std::string& input) {

    		Command command;

    		std::istringstream stream(input);

   		std::string operation;

   		stream >> operation;

    		if (operation == "SET") {

        		command.type = CommandType::SET;

        		stream >> command.key;
       	 		stream >> command.value;

    		}
    		else if (operation == "GET") {

        		command.type = CommandType::GET;

        		stream >> command.key;

    		}
    		else if (operation == "DELETE") {

        		command.type = CommandType::DELETE;

        		stream >> command.key;

    		}
    		else {

        		command.type = CommandType::UNKNOWN;
    		}

    		return command;
	}

}
