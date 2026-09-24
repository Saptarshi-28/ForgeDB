#include "storage/SSTable.h"
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

int main()
{
    using namespace forgedb::storage;

    const std::string db_file =
        "sstable_indexed_lookup_test.db";

    const std::string bf_file =
        "sstable_indexed_lookup_test.bf";

    const std::string idx_file =
        "sstable_indexed_lookup_test.idx";

    std::vector<SSTableEntry> entries;

    for (int i = 1; i <= 500; ++i) {

        char key_buffer[16];
        char value_buffer[16];

        std::snprintf(
            key_buffer,
            sizeof(key_buffer),
            "key%04d",
            i
        );

        std::snprintf(
            value_buffer,
            sizeof(value_buffer),
            "value%04d",
            i
        );

        entries.push_back(
            SSTableEntry{
                key_buffer,
                value_buffer,
                false
            }
        );
    }

    SSTable::write(
        db_file,
        entries
    );

    std::cout
        << "key0001: "
        << SSTable::get(db_file, "key0001")
        << '\n';

    std::cout
        << "key0250: "
        << SSTable::get(db_file, "key0250")
        << '\n';

    std::cout
        << "key0499: "
        << SSTable::get(db_file, "key0499")
        << '\n';

    std::cout
        << "missing: "
        << SSTable::get(db_file, "key9999")
        << '\n';
    
    std::filesystem::remove(idx_file);

    std::cout
        << "fallback key0250: "
        << SSTable::get(db_file, "key0250")
        << '\n';

    std::filesystem::remove(db_file);
    std::filesystem::remove(bf_file);
    std::filesystem::remove(idx_file);

    return 0;
}