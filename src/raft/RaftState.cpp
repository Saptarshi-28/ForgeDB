#include "raft/RaftState.h"

#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <stdexcept>
#include <unistd.h>

namespace forgedb::raft {

RaftState::RaftState(
    const std::string& node_id,
    const std::string& filename
)
    : node_id_(node_id),
      filename_(filename)
{
    load();
}

const std::string&
RaftState::nodeId() const
{
    return node_id_;
}

uint64_t RaftState::currentTerm() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return current_term_;
}

std::string RaftState::votedFor() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return voted_for_;
}

RaftRole RaftState::role() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return role_;
}

void RaftState::setRole(
    RaftRole role
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    role_ = role;
}

void RaftState::updateTerm(
    uint64_t new_term
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (
        new_term <=
        current_term_
    ) {
        return;
    }

    current_term_ =
        new_term;

    /*
     * A node may vote once per term.
     *
     * Entering a newer term therefore
     * clears the previous vote.
     */
    voted_for_.clear();

    role_ =
        RaftRole::FOLLOWER;

    persist();
}

bool RaftState::recordVote(
    const std::string& candidate_id
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (
        !voted_for_.empty() &&
        voted_for_ != candidate_id
    ) {
        return false;
    }

    voted_for_ =
        candidate_id;

    persist();

    return true;
}

void RaftState::load()
{
    std::ifstream file(
        filename_
    );

    if (!file.is_open()) {
        return;
    }

    uint64_t term = 0;
    std::string voted_for;

    file >> term;
    file >> voted_for;

    if (file.fail()) {
        throw std::runtime_error(
            "Invalid Raft state"
        );
    }

    current_term_ =
        term;

    if (
        voted_for != "-"
    ) {
        voted_for_ =
            voted_for;
    }
}

void RaftState::persist() const
{
    std::string temporary =
        filename_ + ".tmp";

    std::string vote =
        voted_for_.empty()
            ? "-"
            : voted_for_;

    std::string data =
        std::to_string(
            current_term_
        ) +
        " " +
        vote +
        "\n";

    int fd = open(
        temporary.c_str(),
        O_WRONLY |
        O_CREAT |
        O_TRUNC,
        0644
    );

    if (fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft state"
        );
    }

    std::size_t total_written = 0;

    while (
        total_written <
        data.size()
    ) {

        ssize_t written =
            write(
                fd,
                data.data() +
                    total_written,
                data.size() -
                    total_written
            );

        if (written < 0) {

            if (errno == EINTR) {
                continue;
            }

            close(fd);
            unlink(
                temporary.c_str()
            );

            throw std::runtime_error(
                "Failed to write Raft state"
            );
        }

        if (written == 0) {

            close(fd);
            unlink(
                temporary.c_str()
            );

            throw std::runtime_error(
                "Raft state write made no progress"
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
        unlink(
            temporary.c_str()
        );

        throw std::runtime_error(
            "Failed to sync Raft state"
        );
    }

    close(fd);

    if (
        rename(
            temporary.c_str(),
            filename_.c_str()
        ) != 0
    ) {

        unlink(
            temporary.c_str()
        );

        throw std::runtime_error(
            "Failed to publish Raft state"
        );
    }

    int directory_fd =
        open(
            ".",
            O_RDONLY |
            O_DIRECTORY
        );

    if (directory_fd < 0) {
        throw std::runtime_error(
            "Failed to open Raft directory"
        );
    }

    while (
        fsync(directory_fd) < 0
    ) {

        if (errno == EINTR) {
            continue;
        }

        close(directory_fd);

        throw std::runtime_error(
            "Failed to sync Raft directory"
        );
    }

    close(directory_fd);
}

}