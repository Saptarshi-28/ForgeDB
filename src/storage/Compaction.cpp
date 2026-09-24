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

    // Read oldest -> newest.
    // Newer entries overwrite older entries for the same key.
    for (const auto& filename : input_files) {

        auto entries = SSTable::readAll(filename);

        for (const auto& entry : entries) {
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

    // Write the new compacted SSTable first.
    // SSTable::write() also creates its Bloom filter.
    SSTable::write(
        output_file,
        merged_entries
    );

    // Only after the new SSTable is safely written
    // do we remove the old SSTables and Bloom filters.
    for (const auto& filename : input_files) {

        std::filesystem::remove(filename);

        std::string bloom_filename = filename;

        if (bloom_filename.ends_with(".db")) {
            bloom_filename.replace(
                bloom_filename.size() - 3,
                3,
                ".bf"
            );

            std::filesystem::remove(bloom_filename);
        }
    }

    // Persist deletion of the old files.
    int dir_fd = open(
        ".",
        O_RDONLY | O_DIRECTORY
    );

    if (dir_fd < 0) {
        throw std::runtime_error(
            "Failed to open compaction directory"
        );
    }

    if (fsync(dir_fd) < 0) {
        close(dir_fd);

        throw std::runtime_error(
            "Failed to sync compaction directory"
        );
    }

    close(dir_fd);
}

}