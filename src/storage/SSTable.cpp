#include "storage/SSTable.h"
#include <optional>
#include <fstream>
#include <stdexcept>
#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include "storage/BloomFilter.h"
#include <algorithm>
#include "storage/SSTableIndex.h"

namespace forgedb::storage {

    void SSTable::write(
        const std::string& filename,
        const std::vector<SSTableEntry>& entries
    )
    {
        std::string temp_filename = filename + ".tmp";

        int fd = open(
            temp_filename.c_str(),
            O_WRONLY | O_CREAT | O_TRUNC,
            0644
        );

        if (fd < 0) {
            throw std::runtime_error("Failed to open SSTable file");
        }
        SSTableIndex index;

        constexpr std::size_t INDEX_INTERVAL = 100;

        std::uint64_t current_offset = 0;
        std::size_t record_number = 0;

        for (const auto& entry : entries) {

            std::string record;

            if (entry.deleted) {
                record = "D\t" + entry.key + "\n";
            }
            else {
                record =
                    "V\t" +
                    entry.key +
                    "\t" +
                    entry.value +
                    "\n";
            }

            if (record_number % INDEX_INTERVAL == 0) {
                index.add(
                    entry.key,
                    current_offset
                );
            }
            std::size_t total_written = 0;

            while (total_written < record.size()) {

                ssize_t bytes_written = ::write(
                    fd,
                    record.data() + total_written,
                    record.size() - total_written
                );

                if (bytes_written < 0) {
                    close(fd);
                    throw std::runtime_error(
                        "Failed to write SSTable"
                    );
                }

                total_written += bytes_written;
            }

            current_offset +=
                static_cast<std::uint64_t>(record.size());

            ++record_number;
        }

        if (fsync(fd) < 0) {
            close(fd);
            throw std::runtime_error(
                "Failed to sync SSTable"
            );
        }

        close(fd);

        if (std::rename(
                temp_filename.c_str(),
                filename.c_str()
            ) != 0) {

            throw std::runtime_error(
                "Failed to rename SSTable"
            );
        }

        int dir_fd = open(
            ".",
            O_RDONLY | O_DIRECTORY
        );

        if (dir_fd < 0) {
            throw std::runtime_error(
                "Failed to open SSTable directory"
            );
        }

        if (fsync(dir_fd) < 0) {
            close(dir_fd);

            throw std::runtime_error(
                "Failed to sync SSTable directory"
            );
        }

        close(dir_fd);

        // Build Bloom filter only after the SSTable
        // has been successfully published.
        std::size_t bloom_bit_count =
            std::max<std::size_t>(
                1024,
                entries.size() * 10
            );

        BloomFilter bloom(
            bloom_bit_count,
            7
        );

        for (const auto& entry : entries) {
            bloom.add(entry.key);
        }

        std::string bloom_filename = filename;

        if (bloom_filename.ends_with(".db")) {
            bloom_filename.replace(
                bloom_filename.size() - 3,
                3,
                ".bf"
            );
        }
        else {
            bloom_filename += ".bf";
        }

        bloom.save(bloom_filename);

        std::string index_filename = filename;

        if (index_filename.ends_with(".db")) {
            index_filename.replace(
                index_filename.size() - 3,
                3,
                ".idx"
            );
        }
        else {
            index_filename += ".idx";
        }

        index.save(index_filename);
    }

    std::string SSTable::get(
        const std::string& filename,
        const std::string& key
    )
    {
        auto result = lookup(filename, key);

        if (!result.has_value()) {
            return "";
        }

        if (result->deleted) {
            return "";
        }

        return result->value;
    }

    std::optional<SSTableEntry> SSTable::lookup(
        const std::string& filename,
        const std::string& key
    )
    {
        std::string bloom_filename = filename;

        if (bloom_filename.ends_with(".db")) {
            bloom_filename.replace(
                bloom_filename.size() - 3,
                3,
                ".bf"
            );
        }
        else {
            bloom_filename += ".bf";
        }

        try {
            BloomFilter bloom =
                BloomFilter::load(bloom_filename);

            if (!bloom.possiblyContains(key)) {
                return std::nullopt;
            }
        }
        catch (...) {
            // Bloom filter is only an optimization.
            // If it is missing or corrupt, fall back
            // to scanning the SSTable normally.
        }

        std::uint64_t start_offset = 0;

        std::string index_filename = filename;

        if (index_filename.ends_with(".db")) {
            index_filename.replace(
                index_filename.size() - 3,
                3,
                ".idx"
            );
        }
        else {
            index_filename += ".idx";
        }

        try {
            SSTableIndex index =
                SSTableIndex::load(index_filename);

            start_offset =
                index.findOffset(key);
        }
        catch (...) {
            // Index is only an optimization.
            // If it is missing or corrupt,
            // scan from the beginning.
            start_offset = 0;
        }

        std::ifstream file(filename, std::ios::binary);

        if (!file.is_open()) {
            return std::nullopt;
        }

        file.seekg(
            static_cast<std::streamoff>(start_offset)
        );

        if (!file) {
            file.clear();
            file.seekg(0);
        }

        std::string line;

        while (std::getline(file, line)) {

            if (line.size() < 3) {
                continue;
            }

            char type = line[0];

            if (line[1] != '\t') {
                continue;
            }

            std::size_t key_end = line.find('\t', 2);

            std::string current_key;

            if (type == 'D') {
                current_key = line.substr(2);
            }
            else if (type == 'V') {

                if (key_end == std::string::npos) {
                    continue;
                }

                current_key = line.substr(2, key_end - 2);
            }
            else {
                continue;
            }

            if (current_key == key) {

                if (type == 'D') {
                    return SSTableEntry{
                        current_key,
                        "",
                        true
                    };
                }

                return SSTableEntry{
                    current_key,
                    line.substr(key_end + 1),
                    false
                };
            }

            if (current_key > key) {
                break;
            }
        }

        return std::nullopt;
    }

    std::vector<SSTableEntry> SSTable::readAll(
        const std::string& filename
    )
    {
        std::vector<SSTableEntry> entries;

        std::ifstream file(filename);

        if (!file.is_open()) {
            return entries;
        }

        std::string line;

        while (std::getline(file, line)) {

            if (line.size() < 3) {
                continue;
            }

            char type = line[0];

            if (line[1] != '\t') {
                continue;
            }

            if (type == 'D') {

                std::string key = line.substr(2);

                entries.push_back(
                    SSTableEntry{
                        key,
                        "",
                        true
                    }
                );
            }
            else if (type == 'V') {

                std::size_t key_end = line.find('\t', 2);

                if (key_end == std::string::npos) {
                    continue;
                }

                std::string key =
                    line.substr(2, key_end - 2);

                std::string value =
                    line.substr(key_end + 1);

                entries.push_back(
                    SSTableEntry{
                        key,
                        value,
                        false
                    }
                );
            }
        }

        return entries;
    }
}
