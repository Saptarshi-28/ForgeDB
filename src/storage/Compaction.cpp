#include "storage/Compaction.h"
#include "storage/SSTable.h"
#include <filesystem>
#include <algorithm>
#include <unordered_map>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>

namespace forgedb::storage {

void Compaction::compact(
    const std::vector<std::string>& input_files,
    const std::string& output_file
)
{
    std::unordered_map<std::string, SSTableEntry> latest_entries;

    // input_files must be ordered oldest -> newest.
    for (const auto& filename : input_files) {

        auto entries = SSTable::readAll(filename);

        for (const auto& entry : entries) {
            // Newer SSTables overwrite older entries
            // for the same key.
            latest_entries[entry.key] = entry;
        }
    }

    std::vector<SSTableEntry> merged_entries;

    merged_entries.reserve(latest_entries.size());

    for (const auto& [key, entry] : latest_entries) {
        merged_entries.push_back(entry);
    }

    std::sort(
        merged_entries.begin(),
        merged_entries.end(),
        [](const SSTableEntry& a, const SSTableEntry& b) {
            return a.key < b.key;
        }
    );

    SSTable::write(
        output_file,
        merged_entries
    );

    for (const auto& filename : input_files) {
        std::filesystem::remove(filename);
    }

    int dir_fd = open(".", O_RDONLY | O_DIRECTORY);

    if (dir_fd < 0) {
        throw std::runtime_error("Failed to open compaction directory");
    }
    
    if (fsync(dir_fd) < 0) {
        close(dir_fd);
        throw std::runtime_error("Failed to sync compaction directory");
    }
    
    close(dir_fd);
}

}