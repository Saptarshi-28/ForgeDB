#include "storage/SSTable.h"

#include <filesystem>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::storage;

    const std::string filename = "sstable_test.db";

    std::vector<SSTableEntry> entries = {
        {"key0001", "value0001", false},
        {"key0500", "value0500", false},
        {"key1000", "value1000", false}
    };

    SSTable::write(
        filename,
        entries
    );

    std::cout << SSTable::get(
        filename,
        "key0001"
    ) << '\n';

    std::cout << SSTable::get(
        filename,
        "key0500"
    ) << '\n';

    std::cout << SSTable::get(
        filename,
        "key1000"
    ) << '\n';

    std::cout << SSTable::get(
        filename,
        "does_not_exist"
    ) << '\n';

    std::filesystem::remove(filename);

    return 0;
}