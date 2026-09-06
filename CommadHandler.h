#include <string>
#include "commands/Commands.h"
#include "storage/KeyValueStore.h"

class CommandHandler {
public:
    CommandHandler(KeyValueStore& store);

    std::string execute(const Command& command);

private:
    KeyValueStore& store_;
};
