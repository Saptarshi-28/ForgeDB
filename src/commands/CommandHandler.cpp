#include "commands/CommandHandler.h"

namespace forgedb::commands {

    CommandHandler::CommandHandler(forgedb::storage::KeyValueStore& store): store_(store)
    {
    }
    std::string CommandHandler::execute(const Command& command)
    {
        switch (command.type) {
            case CommandType::SET:{
                store_.set(command.key, command.value);
                return "OK\n";
            }
            case CommandType::GET:{
                std::string result = store_.get(command.key);
                if (result.empty()) result = "(nil)\n";
                else result += "\n";
                return result;
            }
            case CommandType::DELETE:{
                bool removed = store_.remove(command.key);
                return removed ? "OK\n" : "(nil)\n";
            }
            default:
                return "ERR unknown command\n";
        }
    }
}