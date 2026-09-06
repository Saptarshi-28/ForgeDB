#include "commands/CommandParser.h"
#include "network/Server.h"
#include <iostream>

int main() {

    forgedb::network::Server server;
    server.start();

    return 0;
}
