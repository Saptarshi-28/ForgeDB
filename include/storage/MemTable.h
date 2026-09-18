#pragma once
#include <vector>
#include <utility>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "storage/SSTable.h"

namespace forgedb::storage {

class MemTable {
public:
    void set(const std::string& key, const std::string& value);
    std::string get(const std::string& key) const;
    bool remove(const std::string& key);
    
    bool contains(const std::string& key) const;
    std::size_t size() const;
    bool shouldFlush(std::size_t max_entries) const;
    
    std::vector<SSTableEntry> snapshot() const;
    bool isDeleted(const std::string& key) const;
    void clear();

private:
    std::unordered_map<std::string, std::string> data_;
    std::unordered_set<std::string> tombstones_;
};

}