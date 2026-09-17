#include "storage/WAL.h"
#include <fstream>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>

namespace forgedb::storage{

    WAL::WAL(const std::string& filename) : filename_(filename)
    {
        fd_ = open(
            filename_.c_str(),
            O_WRONLY | O_CREAT | O_APPEND,
            0644
        );

        if (fd_ < 0) {
            throw std::runtime_error("Failed to open WAL");
        }
    }
    WAL::~WAL()
    {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    void WAL::append(const std::string& operation)
    {
        std::string record = operation + "\n";

        std::size_t total_written = 0;

        while (total_written < record.size()) {

            ssize_t bytes_written = write(
                fd_,
                record.data() + total_written,
                record.size() - total_written
            );

            if (bytes_written < 0) {
                throw std::runtime_error("Failed to write WAL");
            }

            total_written += bytes_written;
        }

        if (fsync(fd_) < 0) {
            throw std::runtime_error("Failed to sync WAL");
        }
    }

    std::vector<std::string> WAL::replay()
    {
        std::vector<std::string> operations;

        std::ifstream wal_file(filename_);

        if (!wal_file.is_open()) return operations;

        std::string operation;

        while (std::getline(wal_file, operation)) {

            // A valid WAL record must end with '\n'.
            // If EOF was reached while reading the record,
            // the final record may be incomplete.
            if (wal_file.eof()) {
                break;
            }

            if (!operation.empty()) {
                operations.push_back(operation);
            }
        }

        wal_file.close();

        return operations;
    }
}