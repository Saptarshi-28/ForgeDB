#include "storage/SSTableIndex.h"
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <stdexcept>
#include <algorithm>

namespace forgedb::storage {

void SSTableIndex::add(
    const std::string& key,
    std::uint64_t offset
)
{
    entries_.push_back(
        IndexEntry{
            key,
            offset
        }
    );
}

std::uint64_t SSTableIndex::findOffset(
    const std::string& key
) const
{
    if (entries_.empty()) {
        return 0;
    }

    auto it = std::upper_bound(
        entries_.begin(),
        entries_.end(),
        key,
        [](const std::string& target,
           const IndexEntry& entry) {

            return target < entry.key;
        }
    );

    if (it == entries_.begin()) {
        return 0;
    }

    --it;

    return it->offset;
}

void SSTableIndex::save(
    const std::string& filename
) const
{
    std::string temp_filename = filename + ".tmp";

    int fd = open(
        temp_filename.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open SSTable index file"
        );
    }

    auto write_all = [fd](const char* data, std::size_t size) {

        std::size_t total_written = 0;

        while (total_written < size) {

            ssize_t bytes_written = ::write(
                fd,
                data + total_written,
                size - total_written
            );

            if (bytes_written < 0) {
                throw std::runtime_error(
                    "Failed to write SSTable index"
                );
            }

            total_written += bytes_written;
        }
    };

    try {
        std::uint64_t entry_count =
            static_cast<std::uint64_t>(entries_.size());

        write_all(
            reinterpret_cast<const char*>(&entry_count),
            sizeof(entry_count)
        );

        for (const auto& entry : entries_) {

            std::uint64_t key_length =
                static_cast<std::uint64_t>(
                    entry.key.size()
                );

            write_all(
                reinterpret_cast<const char*>(&key_length),
                sizeof(key_length)
            );

            write_all(
                entry.key.data(),
                entry.key.size()
            );

            write_all(
                reinterpret_cast<const char*>(&entry.offset),
                sizeof(entry.offset)
            );
        }

        if (fsync(fd) < 0) {
            throw std::runtime_error(
                "Failed to sync SSTable index"
            );
        }
    }
    catch (...) {
        close(fd);
        throw;
    }

    close(fd);

    if (std::rename(
            temp_filename.c_str(),
            filename.c_str()
        ) != 0) {

        throw std::runtime_error(
            "Failed to rename SSTable index"
        );
    }

    int dir_fd = open(
        ".",
        O_RDONLY | O_DIRECTORY
    );

    if (dir_fd < 0) {
        throw std::runtime_error(
            "Failed to open SSTable index directory"
        );
    }

    if (fsync(dir_fd) < 0) {
        close(dir_fd);

        throw std::runtime_error(
            "Failed to sync SSTable index directory"
        );
    }

    close(dir_fd);
}

SSTableIndex SSTableIndex::load(
    const std::string& filename
)
{
    int fd = open(
        filename.c_str(),
        O_RDONLY
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open SSTable index file"
        );
    }

    auto read_all = [fd](char* data, std::size_t size) {

        std::size_t total_read = 0;

        while (total_read < size) {

            ssize_t bytes_read = ::read(
                fd,
                data + total_read,
                size - total_read
            );

            if (bytes_read <= 0) {
                throw std::runtime_error(
                    "Failed to read SSTable index"
                );
            }

            total_read += bytes_read;
        }
    };

    SSTableIndex index;

    try {
        std::uint64_t entry_count;

        read_all(
            reinterpret_cast<char*>(&entry_count),
            sizeof(entry_count)
        );

        for (std::uint64_t i = 0;
             i < entry_count;
             ++i) {

            std::uint64_t key_length;

            read_all(
                reinterpret_cast<char*>(&key_length),
                sizeof(key_length)
            );

            std::string key(
                static_cast<std::size_t>(key_length),
                '\0'
            );

            if (key_length > 0) {
                read_all(
                    key.data(),
                    static_cast<std::size_t>(key_length)
                );
            }

            std::uint64_t offset;

            read_all(
                reinterpret_cast<char*>(&offset),
                sizeof(offset)
            );

            index.add(
                key,
                offset
            );
        }
    }
    catch (...) {
        close(fd);
        throw;
    }

    close(fd);

    return index;
}

}