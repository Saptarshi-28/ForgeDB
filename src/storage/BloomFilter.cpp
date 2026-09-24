#include "storage/BloomFilter.h"
#include <fstream>
#include <stdexcept>
#include <functional>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>

namespace forgedb::storage {

BloomFilter::BloomFilter(
    std::size_t bit_count,
    std::size_t hash_count
)
    : bits_(bit_count, false),
      bit_count_(bit_count),
      hash_count_(hash_count)
{
}

std::size_t BloomFilter::hash(
    const std::string& key,
    std::size_t seed
) const
{
    std::uint64_t hash_value =
        1469598103934665603ULL ^
        static_cast<std::uint64_t>(seed);

    for (unsigned char ch : key) {
        hash_value ^= ch;
        hash_value *= 1099511628211ULL;
    }

    hash_value ^=
        static_cast<std::uint64_t>(seed) *
        0x9e3779b97f4a7c15ULL;

    return static_cast<std::size_t>(hash_value);
}

void BloomFilter::add(const std::string& key)
{
    for (std::size_t i = 0; i < hash_count_; ++i) {

        std::size_t index =
            hash(key, i) % bit_count_;

        bits_[index] = true;
    }
}

bool BloomFilter::possiblyContains(
    const std::string& key
) const
{
    for (std::size_t i = 0; i < hash_count_; ++i) {

        std::size_t index =
            hash(key, i) % bit_count_;

        if (!bits_[index]) {
            return false;
        }
    }

    return true;
}

void BloomFilter::save(const std::string& filename) const
{
    std::string temp_filename = filename + ".tmp";

    int fd = open(
        temp_filename.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Bloom filter file"
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
                    "Failed to write Bloom filter"
                );
            }

            total_written += bytes_written;
        }
    };

    try {
        std::uint64_t bit_count =
            static_cast<std::uint64_t>(bit_count_);

        std::uint64_t hash_count =
            static_cast<std::uint64_t>(hash_count_);

        write_all(
            reinterpret_cast<const char*>(&bit_count),
            sizeof(bit_count)
        );

        write_all(
            reinterpret_cast<const char*>(&hash_count),
            sizeof(hash_count)
        );

        for (bool bit : bits_) {

            char value = bit ? 1 : 0;

            write_all(
                &value,
                sizeof(value)
            );
        }

        if (fsync(fd) < 0) {
            throw std::runtime_error(
                "Failed to sync Bloom filter"
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
            "Failed to rename Bloom filter"
        );
    }

    int dir_fd = open(
        ".",
        O_RDONLY | O_DIRECTORY
    );

    if (dir_fd < 0) {
        throw std::runtime_error(
            "Failed to open Bloom filter directory"
        );
    }

    if (fsync(dir_fd) < 0) {
        close(dir_fd);

        throw std::runtime_error(
            "Failed to sync Bloom filter directory"
        );
    }

    close(dir_fd);
}

BloomFilter BloomFilter::load(
    const std::string& filename
)
{
    std::ifstream file(
        filename,
        std::ios::binary
    );

    if (!file.is_open()) {
        throw std::runtime_error(
            "Failed to open Bloom filter file"
        );
    }

    std::uint64_t bit_count;
    std::uint64_t hash_count;

    file.read(
        reinterpret_cast<char*>(&bit_count),
        sizeof(bit_count)
    );

    file.read(
        reinterpret_cast<char*>(&hash_count),
        sizeof(hash_count)
    );

    if (!file) {
        throw std::runtime_error(
            "Failed to read Bloom filter header"
        );
    }

    BloomFilter filter(
        static_cast<std::size_t>(bit_count),
        static_cast<std::size_t>(hash_count)
    );

    for (std::size_t i = 0;
         i < filter.bit_count_;
         ++i) {

        char value;

        file.read(
            &value,
            sizeof(value)
        );

        if (!file) {
            throw std::runtime_error(
                "Failed to read Bloom filter bits"
            );
        }

        filter.bits_[i] = (value != 0);
    }

    return filter;
}
}