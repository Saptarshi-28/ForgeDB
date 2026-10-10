#include "raft/RaftState.h"
#include <limits>
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

bool RaftState::observeLeader(
    uint64_t leader_term
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    /*
     * A leader from an older term
     * cannot be accepted.
     */
    if (
        leader_term <
        current_term_
    ) {
        return false;
    }

    /*
     * Discovering a newer term means
     * our term/vote state is obsolete.
     */
    if (
        leader_term >
        current_term_
    ) {
        current_term_ =
            leader_term;

        voted_for_.clear();

        /*
         * Term and vote must become
         * durable before acknowledging
         * the newer leader.
         */
        persist();
    }

    /*
     * Even if terms are equal, a
     * candidate receiving AppendEntries
     * from the elected leader steps down.
     */
    role_ =
        RaftRole::FOLLOWER;

    return true;
}

RequestVoteResponse RaftState::handleRequestVote(
    const RequestVoteRequest& request,
    uint64_t local_last_log_index,
    uint64_t local_last_log_term
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    // Reject invalid voting requests.
    if (
        request.term == 0 ||
        request.candidate_id.empty()
    ) {
        return {
            current_term_,
            false
        };
    }

    /*
     * A higher term means our information
     * is outdated.
     */
    if (request.term > current_term_) {

        current_term_ = request.term;

        voted_for_.clear();

        role_ = RaftRole::FOLLOWER;

        // Persist the new term before replying.
        persist();
    }

    /*
     * Reject candidates from older terms.
     */
    if (request.term < current_term_) {

        return {
            current_term_,
            false
        };
    }

    /*
     * Compare log freshness.
     *
     * Higher last-log term wins.
     * If terms match, compare log indexes.
     */
    bool log_up_to_date =
        request.last_log_term >
            local_last_log_term
        ||
        (
            request.last_log_term ==
                local_last_log_term
            &&
            request.last_log_index >=
                local_last_log_index
        );

    if (!log_up_to_date) {

        return {
            current_term_,
            false
        };
    }

    /*
     * One vote per term.
     *
     * Voting again for the SAME candidate
     * is allowed.
     */
    if (
        !voted_for_.empty() &&
        voted_for_ != request.candidate_id
    ) {

        return {
            current_term_,
            false
        };
    }

    /*
     * Record and persist our vote.
     */
    if (voted_for_.empty()) {

        voted_for_ =
            request.candidate_id;

        persist();
    }

    return {
        current_term_,
        true
    };
}
uint64_t RaftState::startElection()
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    if (
        current_term_ ==
        std::numeric_limits<uint64_t>::max()
    ) {

        throw std::runtime_error(
            "Raft term overflow"
        );
    }

    ++current_term_;

    role_ =
        RaftRole::CANDIDATE;

    /*
     * Every candidate votes for itself
     * when beginning a new election.
     */
    voted_for_ =
        node_id_;

    persist();

    return current_term_;
}
}