#include "storage/SSTableIndex.h"

#include <filesystem>
#include <iostream>

int main()
{
    using namespace forgedb::storage;

    const std::string filename = "sstable_index_test.idx";

    {
        SSTableIndex index;

        index.add("key0001", 0);
        index.add("key0101", 2200);
        index.add("key0201", 4400);
        index.add("key0301", 6600);

        index.save(filename);
    }

    SSTableIndex loaded =
        SSTableIndex::load(filename);

    std::cout
        << "key0001 -> "
        << loaded.findOffset("key0001")
        << '\n';

    std::cout
        << "key0050 -> "
        << loaded.findOffset("key0050")
        << '\n';

    std::cout
        << "key0175 -> "
        << loaded.findOffset("key0175")
        << '\n';

    std::cout
        << "key0250 -> "
        << loaded.findOffset("key0250")
        << '\n';

    std::cout
        << "key9999 -> "
        << loaded.findOffset("key9999")
        << '\n';

    std::filesystem::remove(filename);

    return 0;
}