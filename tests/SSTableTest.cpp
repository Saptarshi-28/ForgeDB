#include "storage/SSTable.h"

#include <iostream>

int main()
{
    std::cout << forgedb::storage::SSTable::get(
        "sstable_0.db",
        "key0001"
    ) << '\n';

    std::cout << forgedb::storage::SSTable::get(
        "sstable_0.db",
        "key0500"
    ) << '\n';

    std::cout << forgedb::storage::SSTable::get(
        "sstable_0.db",
        "key1000"
    ) << '\n';

    std::cout << forgedb::storage::SSTable::get(
        "sstable_0.db",
        "does_not_exist"
    ) << '\n';
}