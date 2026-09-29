#include "replication/ReplicationLog.h"

#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace forgedb::replication {

ReplicationLog::ReplicationLog(
    const std::string& filename
)
    : filename_(filename)
{
    auto entries = load();

    if (!entries.empty()) {
        next_sequence_ =
            entries.back().sequence + 1;
    }
}

ReplicationEntry ReplicationLog::append(
    const std::string& command
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );
    ReplicationEntry entry{
        next_sequence_,
        command
    };

    std::string record =
        std::to_string(entry.sequence) +
        "\t" +
        entry.command +
        "\n";

    int fd = open(
        filename_.c_str(),
        O_WRONLY |
        O_CREAT |
        O_APPEND,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open replication log"
        );
    }

    std::size_t total_written = 0;

    while (
        total_written <
        record.size()
    ) {

        ssize_t written = write(
            fd,
            record.data() +
                total_written,
            record.size() -
                total_written
        );

        if (written < 0) {

            if (errno == EINTR) {
                continue;
            }

            close(fd);

            throw std::runtime_error(
                "Failed to write replication log"
            );
        }

        if (written == 0) {

            close(fd);

            throw std::runtime_error(
                "Replication log write "
                "made no progress"
            );
        }

        total_written +=
            static_cast<std::size_t>(
                written
            );
    }

    while (fsync(fd) < 0) {

        if (errno == EINTR) {
            continue;
        }

        close(fd);

        throw std::runtime_error(
            "Failed to sync replication log"
        );
    }

    close(fd);
    ++next_sequence_;

    return entry;
}

std::vector<ReplicationEntry>
ReplicationLog::load() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    std::vector<ReplicationEntry>
        entries;

    std::ifstream file(
        filename_,
        std::ios::binary
    );

    if (!file.is_open()) {
        return entries;
    }

    std::string contents(
        (
            std::istreambuf_iterator<char>(
                file
            )
        ),
        std::istreambuf_iterator<char>()
    );

    /*
     * Ignore an incomplete final record.
     *
     * A crash could leave:
     *
     * 1<TAB>SET a one\n
     * 2<TAB>SET b
     *
     * Only entry 1 is considered valid.
     */
    std::size_t last_newline =
        contents.rfind('\n');

    if (
        last_newline ==
        std::string::npos
    ) {
        return entries;
    }

    contents.resize(
        last_newline + 1
    );

    std::size_t position = 0;

    uint64_t expected_sequence = 1;

    while (
        position <
        contents.size()
    ) {

        std::size_t newline =
            contents.find(
                '\n',
                position
            );

        if (
            newline ==
            std::string::npos
        ) {
            break;
        }

        std::string line =
            contents.substr(
                position,
                newline - position
            );

        position =
            newline + 1;

        if (line.empty()) {
            continue;
        }

        std::size_t separator =
            line.find('\t');

        if (
            separator ==
            std::string::npos
        ) {

            throw std::runtime_error(
                "Invalid replication log record"
            );
        }

        uint64_t sequence;

        try {

            sequence =
                std::stoull(
                    line.substr(
                        0,
                        separator
                    )
                );
        }
        catch (...) {

            throw std::runtime_error(
                "Invalid replication sequence"
            );
        }

        if (
            sequence !=
            expected_sequence
        ) {

            throw std::runtime_error(
                "Non-contiguous replication log"
            );
        }

        std::string command =
            line.substr(
                separator + 1
            );

        if (command.empty()) {

            throw std::runtime_error(
                "Empty replication command"
            );
        }

        entries.push_back(
            ReplicationEntry{
                sequence,
                command
            }
        );

        ++expected_sequence;

    }

    return entries;
}

}