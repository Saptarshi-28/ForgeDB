#pragma once
#include "raft/RaftMessages.h"
#include <cstdint>
#include <mutex>
#include <string>

namespace forgedb::raft {

class RaftLog;

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

class RaftState {
public:
    RaftState(
        const std::string& node_id,
        const std::string& filename
    );

    const std::string& nodeId() const;

    uint64_t currentTerm() const;

    std::string votedFor() const;

    RaftRole role() const;
    bool isLeaderInTerm(
        uint64_t expected_term
    ) const;
    uint64_t startElection();

    bool observeLeader(
        uint64_t leader_term
    );

    void setRole(
        RaftRole role
    );

    void updateTerm(
        uint64_t new_term
    );

    bool recordVote(
        const std::string& candidate_id
    );
    RequestVoteResponse handleRequestVote(
        const RequestVoteRequest& request,
        uint64_t local_last_log_index,
        uint64_t local_last_log_term
    );
    uint64_t appendCommandIfLeader(
        RaftLog& log,
        const std::string& command
    );
    bool becomeLeaderWithNoopIfCandidate(
        uint64_t expected_term,
        RaftLog& log
    );
    RequestVoteResponse handleRequestVoteWithLog(
        const RequestVoteRequest& request,
        const RaftLog& log
    );
private:
    void load();

    void persist() const;

    std::string node_id_;
    std::string filename_;

    uint64_t current_term_ = 0;

    std::string voted_for_;

    RaftRole role_ =
        RaftRole::FOLLOWER;

    mutable std::mutex mutex_;

    RequestVoteResponse handleRequestVoteLocked(
        const RequestVoteRequest& request,
        uint64_t local_last_log_index,
        uint64_t local_last_log_term
    );
};

}