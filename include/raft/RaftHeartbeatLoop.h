#pragma once
#include "raft/RaftLog.h"
#include "raft/RaftPeer.h"
#include "raft/RaftState.h"
#include "raft/RaftLeaderProgress.h"
#include "raft/RaftCommitState.h"
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace forgedb::raft {

class RaftHeartbeatLoop {
public:
    RaftHeartbeatLoop(
        const std::string& node_id,
        RaftState& state,
        RaftLog& log,
        const std::vector<RaftPeer>& peers,
        RaftLeaderProgress& progress,
        RaftCommitState& commit_state,
        std::chrono::milliseconds interval
    );

    ~RaftHeartbeatLoop();

    void start();

    void stop();

    bool running() const;

private:
    void run();

    std::string node_id_;

    RaftState& state_;

    RaftLog& log_;

    std::vector<RaftPeer> peers_;

    RaftLeaderProgress& progress_;
    RaftCommitState& commit_state_;

    std::chrono::milliseconds interval_;

    std::atomic<bool> running_{false};

    std::thread worker_;
};

}