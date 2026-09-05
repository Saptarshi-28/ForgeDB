#include "commands/CommandParser.h"

#include <iostream>

int main() {

    forgedb::commands::CommandParser parser;

    auto command = parser.parse("SET name Saptarshi");

    std::cout << "Command type: "
              << static_cast<int>(command.type)
              << std::endl;

    std::cout << "Key: "
              << command.key
              << std::endl;

    std::cout << "Value: "
              << command.value
              << std::endl;

    return 0;
}
