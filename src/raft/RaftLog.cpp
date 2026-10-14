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
void rewriteLogFile(
    const std::string& filename,
    const std::vector<RaftLogEntry>& entries
)
{
    const std::string temp_filename =
        filename + ".tmp";

    int fd = open(
        temp_filename.c_str(),
        O_WRONLY |
        O_CREAT |
        O_TRUNC,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft log temp file"
        );
    }

    auto closeAndRemove =
        [&]() {
            close(fd);

            unlink(
                temp_filename.c_str()
            );
        };

    for (
        const auto& entry :
        entries
    ) {
        std::string record =
            std::to_string(entry.index) +
            "\t" +
            std::to_string(entry.term) +
            "\t" +
            entry.command +
            "\n";

        std::size_t total_written = 0;

        while (
            total_written <
            record.size()
        ) {
            ssize_t written =
                write(
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

                closeAndRemove();

                throw std::runtime_error(
                    "Failed to rewrite Raft log"
                );
            }

            if (written == 0) {

                closeAndRemove();

                throw std::runtime_error(
                    "Raft log rewrite made no progress"
                );
            }

            total_written +=
                static_cast<std::size_t>(
                    written
                );
        }
    }

    while (
        fsync(fd) < 0
    ) {
        if (errno == EINTR) {
            continue;
        }

        closeAndRemove();

        throw std::runtime_error(
            "Failed to sync rewritten Raft log"
        );
    }

    if (
        close(fd) < 0
    ) {
        unlink(
            temp_filename.c_str()
        );

        throw std::runtime_error(
            "Failed to close rewritten Raft log"
        );
    }

    if (
        rename(
            temp_filename.c_str(),
            filename.c_str()
        ) < 0
    ) {
        unlink(
            temp_filename.c_str()
        );

        throw std::runtime_error(
            "Failed to publish rewritten Raft log"
        );
    }

    /*
     * Make the rename durable.
     */
    syncDirectory(
        filename
    );
}

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
uint64_t readProtectedIndex(
    const std::string& filename
)
{
    std::ifstream file(
        filename,
        std::ios::binary
    );

    if (!file.is_open()) {
        std::error_code error;

        const bool exists =
            std::filesystem::exists(
                filename,
                error
            );

        if (error || exists) {
            throw std::runtime_error(
                "Failed to open Raft commit marker"
            );
        }

        return 0;
    }

    const std::string bytes{
        std::istreambuf_iterator<char>(file),
        std::istreambuf_iterator<char>()
    };

    if (
        file.bad() ||
        bytes.size() < 2 ||
        bytes.back() != '\n' ||
        bytes.find('\n') != bytes.size() - 1
    ) {
        throw std::runtime_error(
            "Invalid Raft commit marker"
        );
    }

    return parseNumber(
        std::string_view(
            bytes.data(),
            bytes.size() - 1
        )
    );
}

void persistProtectedIndex(
    const std::string& filename,
    uint64_t index
)
{
    const std::string temp =
        filename + ".tmp";

    const std::string bytes =
        std::to_string(index) + "\n";

    int fd = open(
        temp.c_str(),
        O_WRONLY | O_CREAT | O_TRUNC,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft commit marker temp file"
        );
    }

    auto fail = [&](const char* message) {
        close(fd);
        unlink(temp.c_str());
        throw std::runtime_error(message);
    };

    std::size_t written = 0;

    while (written < bytes.size()) {
        ssize_t count = write(
            fd,
            bytes.data() + written,
            bytes.size() - written
        );

        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }

            fail("Failed to write Raft commit marker");
        }

        if (count == 0) {
            fail(
                "Raft commit marker write made no progress"
            );
        }

        written +=
            static_cast<std::size_t>(count);
    }

    while (fsync(fd) < 0) {
        if (errno == EINTR) {
            continue;
        }

        fail("Failed to sync Raft commit marker");
    }

    if (close(fd) < 0) {
        unlink(temp.c_str());

        throw std::runtime_error(
            "Failed to close Raft commit marker"
        );
    }

    if (
        rename(
            temp.c_str(),
            filename.c_str()
        ) < 0
    ) {
        unlink(temp.c_str());

        throw std::runtime_error(
            "Failed to publish Raft commit marker"
        );
    }

    syncDirectory(filename);
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

    protected_index_ =
        readProtectedIndex(
            filename_ + ".commit"
        );

    if (
        protected_index_ >
        entries_.size()
    ) {
        throw std::runtime_error(
            "Raft commit marker exceeds recovered log"
        );
    }

    if (!entries_.empty()) {

        next_index_ =
            entries_.back().index + 1;
    }
}
uint64_t RaftLog::protectedIndex() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return protected_index_;
}

void RaftLog::protectCommittedThrough(
    uint64_t index
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

    /*
     * Protected index never decreases.
     */
    if (index <= protected_index_) {
        return;
    }

    /*
     * Cannot protect an entry
     * that doesn't exist locally.
     */
    if (index > entries_.size()) {
        throw std::runtime_error(
            "Cannot protect absent Raft log entries"
        );
    }

    /*
     * If the durable update fails,
     * require recovery before another
     * log mutation.
     */
    append_failed_ = true;

    persistProtectedIndex(
        filename_ + ".commit",
        index
    );

    /*
     * Publish in memory only after
     * the marker is durable.
     */
    protected_index_ = index;

    append_failed_ = false;
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
std::optional<RaftLogEntry>
RaftLog::entryAt(
    uint64_t index
)
{
    /*
     * Index zero is Raft's imaginary
     * entry before the real log.
     */
    if (index == 0) {
        return std::nullopt;
    }

    auto entries =
        load();

    for (
        const auto& entry :
        entries
    ) {
        if (
            entry.index == index
        ) {
            return entry;
        }
    }

    return std::nullopt;
}
std::vector<RaftLogEntry>
RaftLog::entriesFrom(
    uint64_t start_index
)
{
    std::vector<RaftLogEntry> result;

    auto entries =
        load();

    for (
        const auto& entry :
        entries
    ) {
        if (
            entry.index >=
            start_index
        ) {
            result.push_back(
                entry
            );
        }
    }

    return result;
}
void RaftLog::appendFromLeader(
    const std::vector<RaftLogEntry>& incoming_entries
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
        incoming_entries.empty()
    ) {
        return;
    }

    /*
     * Validate the incoming batch first.
     *
     * AppendEntries must contain a
     * contiguous sequence of log entries.
     */
    for (
        std::size_t i = 0;
        i < incoming_entries.size();
        ++i
    ) {
        const auto& entry =
            incoming_entries[i];

        if (
            entry.index == 0 ||
            entry.term == 0 ||
            entry.command.empty() ||
            entry.command.find_first_of(
                "\n\r\t"
            ) != std::string::npos
        ) {
            throw std::runtime_error(
                "Invalid leader Raft log entry"
            );
        }

        if (
            i > 0
        ) {
            const auto& previous =
                incoming_entries[i - 1];

            if (
                entry.index !=
                previous.index + 1
            ) {
                throw std::runtime_error(
                    "Leader Raft entries are not contiguous"
                );
            }

            if (
                entry.term <
                previous.term
            ) {
                throw std::runtime_error(
                    "Leader Raft terms cannot decrease"
                );
            }
        }
    }

    const uint64_t first_index =
        incoming_entries.front().index;

    /*
     * We cannot accept an entry after a
     * missing gap.
     *
     * Example:
     *
     * follower ends at 3
     * leader tries to send entry 7
     *
     * Entries 4-6 are missing.
     */
    if (
        first_index >
        next_index_
    ) {
        throw std::runtime_error(
            "Leader Raft entries contain a gap"
        );
    }

    /*
     * Find the first incoming entry that
     * is either:
     *
     * 1. missing locally, or
     * 2. conflicts with our local entry.
     */
    std::size_t incoming_position = 0;

    while (
        incoming_position <
        incoming_entries.size()
    ) {
        const auto& incoming =
            incoming_entries[
                incoming_position
            ];

        if (
            incoming.index >=
            next_index_
        ) {
            /*
             * Follower is shorter.
             * Everything from here onward
             * must be appended.
             */
            break;
        }

        const auto& local =
            entries_[
                static_cast<std::size_t>(
                    incoming.index - 1
                )
            ];

        /*
         * Same index and term means these
         * should represent the same Raft
         * log entry.
         */
        if (
            local.term ==
            incoming.term
        ) {
            /*
             * Same index + same term but
             * different command would violate
             * Raft's log matching guarantee.
             */
            if (
                local.command !=
                incoming.command
            ) {
                throw std::runtime_error(
                    "Raft log contains same-term command conflict"
                );
            }

            ++incoming_position;

            continue;
        }

        /*
         * Same index but different term:
         * this is the first conflicting
         * entry.
         */
        break;
    }

    /*
     * Every incoming entry already exists
     * identically. This is a retry /
     * duplicate AppendEntries request.
     */
    if (
        incoming_position ==
        incoming_entries.size()
    ) {
        return;
    }

    const uint64_t replacement_index =
        incoming_entries[
            incoming_position
        ].index;

    if (
        replacement_index <= protected_index_
    ) {
        throw std::runtime_error(
            "Cannot replace committed Raft entry"
        );
    }
    std::vector<RaftLogEntry>
        new_entries;

    /*
     * Preserve everything before the
     * first missing/conflicting index.
     */
    for (
        const auto& local :
        entries_
    ) {
        if (
            local.index >=
            replacement_index
        ) {
            break;
        }

        new_entries.push_back(
            local
        );
    }

    /*
     * The first new entry's term cannot
     * go backwards relative to the
     * retained prefix.
     */
    if (
        !new_entries.empty() &&
        incoming_entries[
            incoming_position
        ].term <
        new_entries.back().term
    ) {
        throw std::runtime_error(
            "Leader Raft term precedes retained log term"
        );
    }

    /*
     * Add the leader's authoritative
     * suffix.
     */
    for (
        std::size_t i =
            incoming_position;
        i < incoming_entries.size();
        ++i
    ) {
        new_entries.push_back(
            incoming_entries[i]
        );
    }

    /*
     * Once durable rewriting begins,
     * any failure requires restart /
     * recovery before another mutation.
     */
    append_failed_ = true;

    rewriteLogFile(
        filename_,
        new_entries
    );

    /*
     * Publish only after the rewritten
     * log is durable.
     */
    entries_ =
        std::move(
            new_entries
        );

    if (
        entries_.empty()
    ) {
        next_index_ = 1;
    }
    else {
        next_index_ =
            entries_.back().index + 1;
    }

    append_failed_ = false;
}
void RaftLog::truncateSuffix(
    uint64_t from_index
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

    /*
     * Index zero is only the Raft
     * sentinel and cannot be truncated.
     */
    if (from_index == 0) {
        throw std::runtime_error(
            "Invalid Raft log truncation index"
        );
    }

    if (
        from_index <= protected_index_
    ) {
        throw std::runtime_error(
            "Cannot truncate committed Raft entry"
        );
    }

    /*
     * No entries exist at or beyond
     * this index.
     */
    if (
        from_index >=
        next_index_
    ) {
        return;
    }

    std::vector<RaftLogEntry>
        retained_entries;

    retained_entries.reserve(
        static_cast<std::size_t>(
            from_index - 1
        )
    );

    for (
        const auto& entry :
        entries_
    ) {
        if (
            entry.index >=
            from_index
        ) {
            break;
        }

        retained_entries.push_back(
            entry
        );
    }

    /*
     * Once rewriting begins, any failure
     * requires recovery before another
     * mutation is attempted.
     */
    append_failed_ = true;

    rewriteLogFile(
        filename_,
        retained_entries
    );

    /*
     * Only publish the new state in
     * memory after the durable rewrite
     * succeeds.
     */
    entries_ =
        std::move(
            retained_entries
        );

    if (
        entries_.empty()
    ) {
        next_index_ = 1;
    }
    else {
        next_index_ =
            entries_.back().index + 1;
    }

    append_failed_ = false;
}
}