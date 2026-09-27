#include "storage/Compaction.h"
#include "storage/SSTable.h"
#include <filesystem>
#include <algorithm>
#include <unordered_map>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>
#include <cerrno>
#include <system_error>

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

    for (const auto& filename : input_files) {

        std::error_code ec;

        // Old SSTables are obsolete once the compacted
        // replacement has been durably published.
        // Cleanup failures are therefore non-fatal.
        std::filesystem::remove(
            filename,
            ec
        );

        std::string bloom_filename = filename;

        if (bloom_filename.ends_with(".db")) {
            bloom_filename.replace(
                bloom_filename.size() - 3,
                3,
                ".bf"
            );

            ec.clear();

            std::filesystem::remove(
                bloom_filename,
                ec
            );
        }

        std::string index_filename = filename;

        if (index_filename.ends_with(".db")) {
            index_filename.replace(
                index_filename.size() - 3,
                3,
                ".idx"
            );

            ec.clear();

            std::filesystem::remove(
                index_filename,
                ec
            );
        }
    }

    // Persist deletion of the old files.
    // Best-effort persistence of old-file deletions.
    // The compacted SSTable is already durable,
    // so failure here is not a correctness failure.
    int dir_fd = open(
        ".",
        O_RDONLY | O_DIRECTORY
    );

    if (dir_fd >= 0) {

        while (true) {

            if (::fsync(dir_fd) == 0) {
                break;
            }

            if (errno == EINTR) {
                continue;
            }

            // Cleanup durability failed.
            // Old SSTables may remain after a crash,
            // but the new compacted SSTable is safe.
            break;
        }

        ::close(dir_fd);
    }
}

}