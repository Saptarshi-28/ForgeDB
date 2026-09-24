#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace forgedb::storage {

class BloomFilter {
public:
    BloomFilter(
        std::size_t bit_count,
        std::size_t hash_count
    );

    void add(const std::string& key);

    bool possiblyContains(
        const std::string& key
    ) const;

    void save(const std::string& filename) const;

    static BloomFilter load(
        const std::string& filename
    );

private:
    std::vector<bool> bits_;

    std::size_t bit_count_;
    std::size_t hash_count_;

    std::size_t hash(
        const std::string& key,
        std::size_t seed
    ) const;
};

}