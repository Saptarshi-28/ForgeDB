#include "raft/RaftCommitState.h"

#include <algorithm>
#include <functional>
#include <stdexcept>
#include <vector>

namespace forgedb::raft {

    RaftCommitState::RaftCommitState(
    RaftLog& durable_log
)
    : durable_log_(&durable_log),
      commit_index_(durable_log.protectedIndex())
{
}

RaftCommitState::RaftCommitState(
    VolatileForTests
)
{
}
uint64_t
RaftCommitState::commitIndex() const
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    return commit_index_;
}

uint64_t
RaftCommitState::advanceLeaderCommit(
    uint64_t current_term,
    const std::vector<uint64_t>&
        follower_match_indexes,
    const RaftLog& log
)
{
    if (
        current_term == 0
    ) {
        throw std::invalid_argument(
            "Raft term cannot be zero"
        );
    }
    if (
        durable_log_ != nullptr &&
        durable_log_ != &log
    ) {
        throw std::invalid_argument(
            "RaftCommitState uses a different RaftLog"
        );
    }

    const uint64_t leader_last_index =
        log.lastLogInfo().first;

    std::vector<uint64_t>
        replicated_indexes;

    replicated_indexes.reserve(
        follower_match_indexes.size() + 1
    );

    /*
     * The leader itself always has its
     * own log.
     */
    replicated_indexes.push_back(
        leader_last_index
    );

    for (
        uint64_t match_index :
        follower_match_indexes
    ) {
        if (
            match_index >
            leader_last_index
        ) {
            throw std::runtime_error(
                "Follower matchIndex exceeds leader log"
            );
        }

        replicated_indexes.push_back(
            match_index
        );
    }

    std::sort(
        replicated_indexes.begin(),
        replicated_indexes.end(),
        std::greater<uint64_t>()
    );

    const std::size_t cluster_size =
        replicated_indexes.size();

    const std::size_t majority =
        cluster_size / 2 + 1;

    /*
     * Example:
     *
     * [5, 5, 2]
     *
     * majority = 2
     *
     * candidate =
     * replicated_indexes[1] = 5
     */
    const uint64_t candidate =
        replicated_indexes[
            majority - 1
        ];

    std::lock_guard<std::mutex> lock(
        mutex_
    );

    /*
     * commitIndex never decreases.
     */
    if (
        candidate <=
        commit_index_
    ) {
        return commit_index_;
    }

    auto candidate_term =
        log.termAt(
            candidate
        );

    if (
        !candidate_term.has_value()
    ) {
        throw std::runtime_error(
            "Commit candidate missing from Raft log"
        );
    }

    /*
     * Important Raft safety rule:
     *
     * Leaders only advance commitIndex
     * by replica counting for entries
     * from their CURRENT term.
     */
    if (
        *candidate_term !=
        current_term
    ) {
        return commit_index_;
    }

    if (durable_log_ != nullptr) {
        durable_log_->protectCommittedThrough(
            candidate
        );
    }

    commit_index_ = candidate;

    return commit_index_;
}

uint64_t
RaftCommitState::advanceFollowerCommit(
    uint64_t leader_commit,
    uint64_t follower_last_log_index
)
{
    std::lock_guard<std::mutex> lock(
        mutex_
    );

    /*
     * A follower must never lose an
     * entry it has already committed.
     *
     * If the log is shorter than our
     * existing commitIndex, something
     * has violated Raft's safety rules.
     */
    if (
        follower_last_log_index <
        commit_index_
    ) {
        throw std::runtime_error(
            "Raft log shorter than commitIndex"
        );
    }

    /*
     * The follower cannot commit entries
     * it has not received yet.
     */
    const uint64_t new_commit =
        std::min(
            leader_commit,
            follower_last_log_index
        );

    if (new_commit > commit_index_) {

        /*
         * Confirm the log entry exists
         * and durably protect it first.
         */
        if (durable_log_ != nullptr) {
            durable_log_->protectCommittedThrough(
                new_commit
            );
        }

        /*
         * Publish only after durability.
         */
        commit_index_ = new_commit;
    }

    return commit_index_;
}
}