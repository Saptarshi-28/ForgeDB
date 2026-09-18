#pragma once
#include <optional>
#include <string>
#include <vector>
#include <utility>

namespace forgedb::storage {

    struct SSTableEntry {
    std::string key;
    std::string value;
    bool deleted = false;
};

class SSTable {
public:
    static void write(
        const std::string& filename,
        const std::vector<SSTableEntry>& entries
    );
    static std::string get(
        const std::string& filename,
        const std::string& key
    );
    static std::optional<SSTableEntry> lookup(
        const std::string& filename,
        const std::string& key
    );
};

}