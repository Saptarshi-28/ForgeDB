#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace forgedb::storage {

struct IndexEntry {
    std::string key;
    std::uint64_t offset;
};

class SSTableIndex {
public:
    void add(
        const std::string& key,
        std::uint64_t offset
    );

    std::uint64_t findOffset(
        const std::string& key
    ) const;

    void save(const std::string& filename) const;

    static SSTableIndex load(
        const std::string& filename
    );
    
private:
    std::vector<IndexEntry> entries_;
};

}