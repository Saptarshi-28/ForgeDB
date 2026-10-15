#include "raft/RaftApplyProgress.h"

#include <cerrno>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

namespace forgedb::raft {

namespace {

uint64_t readIndex(const std::string& filename)
{
    std::ifstream file(filename, std::ios::binary);

    if (!file.is_open()) {
        std::error_code ec;
        bool exists = std::filesystem::exists(filename, ec);

        if (ec || exists) {
            throw std::runtime_error(
                "Cannot open Raft applied-index file"
            );
        }

        return 0;
    }

    std::string data{
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()
    };

    if (
        file.bad() ||
        data.size() < 2 ||
        data.back() != '\n' ||
        data.find('\n') != data.size() - 1
    ) {
        throw std::runtime_error(
            "Invalid Raft applied-index file"
        );
    }

    std::string_view number(
        data.data(),
        data.size() - 1
    );

    uint64_t value = 0;

    auto [ptr, ec] = std::from_chars(
        number.data(),
        number.data() + number.size(),
        value
    );

    if (
        ec != std::errc{} ||
        ptr != number.data() + number.size()
    ) {
        throw std::runtime_error(
            "Invalid Raft applied-index number"
        );
    }

    return value;
}

void persistIndex(
    const std::string& filename,
    uint64_t index
)
{
    const std::string temp = filename + ".tmp";
    const std::string data =
        std::to_string(index) + "\n";

    int fd = ::open(
        temp.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Cannot create Raft applied-index temp file"
        );
    }

    auto failWrite = [&](const char* message) {
        ::close(fd);
        ::unlink(temp.c_str());
        throw std::runtime_error(message);
    };

    std::size_t written = 0;

    while (written < data.size()) {
        ssize_t count = ::write(
            fd,
            data.data() + written,
            data.size() - written
        );

        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }

            failWrite("Raft applied-index write failed");
        }

        if (count == 0) {
            failWrite("Raft applied-index write stalled");
        }

        written += static_cast<std::size_t>(count);
    }

    while (::fsync(fd) < 0) {
        if (errno == EINTR) {
            continue;
        }

        failWrite("Raft applied-index fsync failed");
    }

    if (::close(fd) < 0) {
        ::unlink(temp.c_str());

        throw std::runtime_error(
            "Raft applied-index close failed"
        );
    }

    if (::rename(temp.c_str(), filename.c_str()) < 0) {
        ::unlink(temp.c_str());

        throw std::runtime_error(
            "Raft applied-index rename failed"
        );
    }

    auto directory =
        std::filesystem::path(filename).parent_path();

    if (directory.empty()) {
        directory = ".";
    }

    int dirfd = ::open(
        directory.c_str(),
        O_RDONLY | O_DIRECTORY
    );

    if (dirfd < 0) {
        throw std::runtime_error(
            "Cannot open Raft applied-index directory"
        );
    }

    while (::fsync(dirfd) < 0) {
        if (errno == EINTR) {
            continue;
        }

        ::close(dirfd);

        throw std::runtime_error(
            "Raft applied-index directory fsync failed"
        );
    }

    if (::close(dirfd) < 0) {
        throw std::runtime_error(
            "Raft applied-index directory close failed"
        );
    }
}

} // anonymous namespace

RaftApplyProgress::RaftApplyProgress(
    RaftLog& log,
    const std::string& filename
)
    : log_(log),
      filename_(filename),
      last_applied_(readIndex(filename))
{
    if (last_applied_ > log_.protectedIndex()) {
        throw std::runtime_error(
            "lastApplied exceeds durable commitIndex"
        );
    }
}

uint64_t RaftApplyProgress::lastApplied() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return last_applied_;
}

void RaftApplyProgress::recordApplied(uint64_t index)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (failed_) {
        throw std::runtime_error(
            "Raft apply progress requires recovery"
        );
    }

    if (index == last_applied_) {
        return;
    }

    if (index < last_applied_) {
        throw std::runtime_error(
            "Raft lastApplied cannot decrease"
        );
    }

    // Entries must be applied sequentially.
    if (index - last_applied_ != 1) {
        throw std::runtime_error(
            "Raft application index gap"
        );
    }

    // Never record an uncommitted entry as applied.
    if (index > log_.protectedIndex()) {
        throw std::runtime_error(
            "Cannot apply an uncommitted Raft entry"
        );
    }

    /*
     * The caller must already have persisted
     * the corresponding KeyValueStore operation.
     *
     * A persistence failure requires reopening
     * this state from disk before further updates.
     */
    failed_ = true;

    persistIndex(filename_, index);

    last_applied_ = index;
    failed_ = false;
}

}