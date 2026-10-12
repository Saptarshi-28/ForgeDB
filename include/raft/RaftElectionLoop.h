#pragma once

#include "raft/ElectionTimer.h"
#include "raft/RaftLog.h"
#include "raft/RaftPeer.h"
#include "raft/RaftState.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace forgedb::raft {

class RaftElectionLoop {
public:
    RaftElectionLoop(
        const std::string& node_id,
        RaftState& state,
        RaftLog& log,
        const std::vector<RaftPeer>& peers,
        ElectionTimer& election_timer,
        std::chrono::milliseconds poll_interval =
            std::chrono::milliseconds{50}
    );

    ~RaftElectionLoop();

    void start();

    void stop();

    bool running() const;

private:
    void run();

    std::string node_id_;

    RaftState& state_;

    RaftLog& log_;

    std::vector<RaftPeer> peers_;

    ElectionTimer& election_timer_;

    std::chrono::milliseconds poll_interval_;

    std::atomic<bool> running_{
        false
    };

    std::thread worker_;
};

}