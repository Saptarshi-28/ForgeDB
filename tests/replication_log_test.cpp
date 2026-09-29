#include "replication/ReplicationLog.h"

#include <iostream>

int main()
{
    forgedb::replication::ReplicationLog log(
        "replication_test.log"
    );

    auto first =
        log.append(
            "SET alpha one"
        );

    auto second =
        log.append(
            "SET beta two"
        );

    auto third =
        log.append(
            "DELETE alpha"
        );

    std::cout
        << "Appended sequences: "
        << first.sequence
        << ", "
        << second.sequence
        << ", "
        << third.sequence
        << "\n";

    auto entries =
        log.load();

    for (
        const auto& entry :
        entries
    ) {

        std::cout
            << entry.sequence
            << " -> "
            << entry.command
            << "\n";
    }

    return 0;
}