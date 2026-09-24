#include "storage/SSTable.h"

#include <filesystem>
#include <iostream>
#include <vector>

int main()
{
    using namespace forgedb::storage;

    const std::string db_file = "bloom_sstable_test.db";
    const std::string bf_file = "bloom_sstable_test.bf";
    const std::string idx_file = "bloom_sstable_test.idx";
    std::vector<SSTableEntry> entries = {
        {"apple", "red", false},
        {"banana", "yellow", false},
        {"orange", "orange", false}
    };

    SSTable::write(
        db_file,
        entries
    );

    std::cout
        << "existing: "
        << SSTable::get(db_file, "banana")
        << '\n';

    std::cout
        << "missing: "
        << SSTable::get(db_file, "grape")
        << '\n';

    // Remove only the Bloom filter.
    // SSTable lookup must still work correctly.
    std::filesystem::remove(bf_file);

    std::cout
        << "fallback: "
        << SSTable::get(db_file, "apple")
        << '\n';

    std::filesystem::remove(db_file);
    std::filesystem::remove(bf_file);
    std::filesystem::remove(idx_file);
    return 0;
}