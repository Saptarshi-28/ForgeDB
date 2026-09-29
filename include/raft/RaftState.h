#pragma once

#include <cstdint>
#include <mutex>
#include <string>

namespace forgedb::raft {

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

    void setRole(
        RaftRole role
    );

    void updateTerm(
        uint64_t new_term
    );

    bool recordVote(
        const std::string& candidate_id
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
};

}