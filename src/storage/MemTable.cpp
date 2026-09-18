#include "storage/MemTable.h"
#include <algorithm>

namespace forgedb::storage {

    void MemTable::set(const std::string& key, const std::string& value)
    {
        tombstones_.erase(key);
        data_[key] = value;
    }

    std::string MemTable::get(const std::string& key) const
    {
        auto it = data_.find(key);

        if (it == data_.end()) {
            return "";
        }

        return it->second;
    }

    bool MemTable::remove(const std::string& key)
    {
        bool existed = data_.erase(key) > 0;

        tombstones_.insert(key);

        return existed;
    }

    bool MemTable::contains(const std::string& key) const
    {
        return data_.find(key) != data_.end();
    }

    std::size_t MemTable::size() const
    {
        return data_.size() + tombstones_.size();
    }

    bool MemTable::shouldFlush(std::size_t max_entries) const
    {
        return data_.size() >= max_entries;
    }

    std::vector<SSTableEntry> MemTable::snapshot() const
    {
        std::vector<SSTableEntry> entries;

        entries.reserve(data_.size() + tombstones_.size());

        for (const auto& [key, value] : data_) {
            entries.push_back(
                SSTableEntry{
                    key,
                    value,
                    false
                }
            );
        }

        for (const auto& key : tombstones_) {
            entries.push_back(
                SSTableEntry{
                    key,
                    "",
                    true
                }
            );
        }

        std::sort(
            entries.begin(),
            entries.end(),
            [](const SSTableEntry& a, const SSTableEntry& b) {
                return a.key < b.key;
            }
        );

        return entries;
    }

    bool MemTable::isDeleted(const std::string& key) const
    {
        return tombstones_.find(key) != tombstones_.end();
    }

    void MemTable::clear()
    {
        data_.clear();
        tombstones_.clear();
    }
}