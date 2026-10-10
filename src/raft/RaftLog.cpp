#include "raft/RaftLog.h"

#include <cerrno>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

namespace forgedb::raft {

namespace {

struct ParsedLog {
    std::vector<RaftLogEntry> entries;

    std::size_t complete_bytes = 0;
    std::size_t total_bytes = 0;
};

uint64_t parseNumber(std::string_view token)
{
    uint64_t value = 0;

    if (token.empty()) {
        throw std::runtime_error(
            "Empty Raft log number"
        );
    }

    auto [ptr, error] = std::from_chars(
        token.data(),
        token.data() + token.size(),
        value
    );

    if (
        error != std::errc{} ||
        ptr != token.data() + token.size()
    ) {
        throw std::runtime_error(
            "Invalid Raft log number"
        );
    }

    return value;
}

ParsedLog readLogFile(
    const std::string& filename
)
{
    ParsedLog result;

    std::ifstream file(
        filename,
        std::ios::binary
    );

    if (!file.is_open()) {

        std::error_code error;

        bool exists =
            std::filesystem::exists(
                filename,
                error
            );

        if (error || exists) {
            throw std::runtime_error(
                "Failed to open Raft log"
            );
        }

        return result;
    }

    std::string contents{
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()
    };

    if (file.bad()) {
        throw std::runtime_error(
            "Failed to read Raft log"
        );
    }

    result.total_bytes = contents.size();

    auto last_newline =
        contents.rfind('\n');

    if (last_newline == std::string::npos) {
        return result;
    }

    result.complete_bytes =
        last_newline + 1;

    std::size_t position = 0;

    uint64_t expected_index = 1;
    uint64_t previous_term = 0;

    while (
        position < result.complete_bytes
    ) {

        auto newline =
            contents.find(
                '\n',
                position
            );

        std::string line =
            contents.substr(
                position,
                newline - position
            );

        position = newline + 1;

        auto first_tab =
            line.find('\t');

        if (first_tab == std::string::npos) {
            throw std::runtime_error(
                "Invalid Raft log record"
            );
        }

        auto second_tab =
            line.find(
                '\t',
                first_tab + 1
            );

        if (second_tab == std::string::npos) {
            throw std::runtime_error(
                "Missing Raft log term/command"
            );
        }

        if (
            line.find(
                '\t',
                second_tab + 1
            ) != std::string::npos
        ) {
            throw std::runtime_error(
                "Invalid Raft log command"
            );
        }

        uint64_t index = parseNumber(
            std::string_view(
                line.data(),
                first_tab
            )
        );

        uint64_t term = parseNumber(
            std::string_view(
                line.data() + first_tab + 1,
                second_tab - first_tab - 1
            )
        );

        std::string command =
            line.substr(
                second_tab + 1
            );

        if (
            index != expected_index ||
            term == 0 ||
            term < previous_term ||
            command.empty()
        ) {
            throw std::runtime_error(
                "Invalid Raft log sequence"
            );
        }

        result.entries.push_back({
            index,
            term,
            command
        });

        previous_term = term;

        if (
            expected_index ==
            std::numeric_limits<uint64_t>::max()
        ) {
            throw std::runtime_error(
                "Raft log index overflow"
            );
        }

        ++expected_index;
    }

    return result;
}

void syncDirectory(
    const std::string& filename
)
{
    auto directory =
        std::filesystem::path(filename)
            .parent_path();

    if (directory.empty()) {
        directory = ".";
    }

    int fd = open(
        directory.c_str(),
        O_RDONLY | O_DIRECTORY
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft log directory"
        );
    }

    while (fsync(fd) < 0) {

        if (errno == EINTR) {
            continue;
        }

        close(fd);

        throw std::runtime_error(
            "Failed to sync Raft log directory"
        );
    }

    if (close(fd) < 0) {
        throw std::runtime_error(
            "Failed to close Raft log directory"
        );
    }
}

void repairIncompleteTail(
    const std::string& filename,
    std::size_t valid_bytes
)
{
    int fd = open(
        filename.c_str(),
        O_WRONLY
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft log for repair"
        );
    }

    while (
        ftruncate(
            fd,
            static_cast<off_t>(valid_bytes)
        ) < 0
    ) {

        if (errno == EINTR) {
            continue;
        }

        close(fd);

        throw std::runtime_error(
            "Failed to repair Raft log"
        );
    }

    while (fsync(fd) < 0) {

        if (errno == EINTR) {
            continue;
        }

        close(fd);

        throw std::runtime_error(
            "Failed to sync repaired Raft log"
        );
    }

    if (close(fd) < 0) {
        throw std::runtime_error(
            "Failed to close repaired Raft log"
        );
    }
}

} // anonymous namespace

RaftLog::RaftLog(
    const std::string& filename
)
    : filename_(filename)
{
    auto recovered =
        readLogFile(filename_);

    /*
     * If a crash left an incomplete final
     * record, remove those bytes BEFORE
     * permitting another append.
     */
    if (
        recovered.complete_bytes <
        recovered.total_bytes
    ) {

        repairIncompleteTail(
            filename_,
            recovered.complete_bytes
        );
    }

    entries_ =
        std::move(recovered.entries);

    if (!entries_.empty()) {

        next_index_ =
            entries_.back().index + 1;
    }
}

RaftLogEntry RaftLog::append(
    uint64_t term,
    const std::string& command
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (append_failed_) {
        throw std::runtime_error(
            "Raft log requires recovery"
        );
    }

    if (
        term == 0 ||
        command.empty() ||
        command.find_first_of("\n\r\t") !=
            std::string::npos
    ) {
        throw std::runtime_error(
            "Invalid Raft log entry"
        );
    }

    if (
        !entries_.empty() &&
        term < entries_.back().term
    ) {
        throw std::runtime_error(
            "Raft log term cannot decrease"
        );
    }

    RaftLogEntry entry{
        next_index_,
        term,
        command
    };

    std::string record =
        std::to_string(entry.index) +
        "\t" +
        std::to_string(entry.term) +
        "\t" +
        entry.command +
        "\n";

    /*
     * Once writing begins, any failure
     * requires recovery before another
     * append is permitted.
     */
    append_failed_ = true;

    int fd = open(
        filename_.c_str(),
        O_WRONLY | O_CREAT | O_APPEND,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft log"
        );
    }

    std::size_t total_written = 0;

    while (
        total_written < record.size()
    ) {

        ssize_t written = write(
            fd,
            record.data() + total_written,
            record.size() - total_written
        );

        if (written < 0) {

            if (errno == EINTR) {
                continue;
            }

            close(fd);

            throw std::runtime_error(
                "Failed to write Raft log"
            );
        }

        if (written == 0) {

            close(fd);

            throw std::runtime_error(
                "Raft log write made no progress"
            );
        }

        total_written +=
            static_cast<std::size_t>(written);
    }

    while (fsync(fd) < 0) {

        if (errno == EINTR) {
            continue;
        }

        close(fd);

        throw std::runtime_error(
            "Failed to sync Raft log"
        );
    }

    if (close(fd) < 0) {
        throw std::runtime_error(
            "Failed to close Raft log"
        );
    }

    syncDirectory(filename_);

    /*
     * Only publish the entry in memory
     * after its durable write succeeds.
     */
    entries_.push_back(entry);

    ++next_index_;

    append_failed_ = false;

    return entry;
}

std::vector<RaftLogEntry>
RaftLog::load() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return readLogFile(filename_).entries;
}

std::pair<uint64_t, uint64_t>
RaftLog::lastLogInfo() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (entries_.empty()) {
        return {0, 0};
    }

    const auto& last =
        entries_.back();

    return {
        last.index,
        last.term
    };
}

std::optional<uint64_t>
RaftLog::termAt(
    uint64_t index
) const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    /*
     * Raft uses index 0 as the
     * "before the beginning of the log"
     * sentinel.
     *
     * There is no real entry at index 0.
     */
    if (index == 0) {
        return uint64_t{0};
    }

    if (
        index >
        entries_.size()
    ) {
        return std::nullopt;
    }

    /*
     * Log indexes start at 1,
     * vector positions start at 0.
     */
    return entries_[
        static_cast<std::size_t>(
            index - 1
        )
    ].term;
}

}