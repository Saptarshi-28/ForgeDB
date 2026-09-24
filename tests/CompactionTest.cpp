#include "storage/SSTable.h"
#include "storage/Compaction.h"
#include <filesystem>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::storage;

    std::vector<SSTableEntry> old_entries = {
        {"city", "Delhi", false},
        {"name", "old", false},
        {"theme", "dark", false}
    };

    std::vector<SSTableEntry> new_entries = {
        {"city", "", true},
        {"name", "new", false},
        {"version", "2", false}
    };

    SSTable::write(
        "compaction_old.db",
        old_entries
    );

    SSTable::write(
        "compaction_new.db",
        new_entries
    );

    Compaction::compact(
        {
            "compaction_old.db",
            "compaction_new.db"
        },
        "compaction_output.db"
    );

    auto entries =
        SSTable::readAll("compaction_output.db");

    for (const auto& entry : entries) {

        if (entry.deleted) {
            std::cout
                << "D "
                << entry.key
                << '\n';
        }
        else {
            std::cout
                << "V "
                << entry.key
                << " "
                << entry.value
                << '\n';
        }
    }
    std::filesystem::remove("compaction_output.db");
    std::filesystem::remove("compaction_output.bf");
    std::filesystem::remove("compaction_output.idx");
    return 0;
}