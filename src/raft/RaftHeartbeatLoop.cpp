#include "raft/RaftHeartbeatLoop.h"
#include "raft/RaftHeartbeat.h"

#include <stdexcept>
#include <thread>

namespace forgedb::raft {

RaftHeartbeatLoop::RaftHeartbeatLoop(
    const std::string& node_id,
    RaftState& state,
    RaftLog& log,
    const std::vector<RaftPeer>& peers,
    std::chrono::milliseconds interval
)
    : node_id_(node_id),
      state_(state),
      log_(log),
      peers_(peers),
      interval_(interval)
{
    if (node_id_.empty()) {
        throw std::invalid_argument(
            "Raft node ID cannot be empty"
        );
    }

    if (
        interval_.count() <= 0
    ) {
        throw std::invalid_argument(
            "Invalid heartbeat interval"
        );
    }
}

RaftHeartbeatLoop::~RaftHeartbeatLoop()
{
    stop();
}

void RaftHeartbeatLoop::start()
{
    bool expected = false;

    if (
        !running_.compare_exchange_strong(
            expected,
            true
        )
    ) {
        return;
    }

    worker_ =
        std::thread(
            &RaftHeartbeatLoop::run,
            this
        );
}

void RaftHeartbeatLoop::stop()
{
    running_.store(false);

    if (
        worker_.joinable()
    ) {
        worker_.join();
    }
}

bool RaftHeartbeatLoop::running() const
{
    return running_.load();
}

void RaftHeartbeatLoop::run()
{
    RaftHeartbeat heartbeat(
        node_id_,
        state_,
        log_,
        peers_
    );

    while (
        running_.load()
    ) {
        /*
         * Followers and candidates do
         * not send leader heartbeats.
         */
        if (
            state_.role() ==
            RaftRole::LEADER
        ) {
            auto result =
                heartbeat.sendHeartbeats();

            /*
             * sendHeartbeats() already
             * updates RaftState when a
             * peer reports a newer term.
             */
            if (
                result.stepped_down
            ) {
                /*
                 * Nothing else to do.
                 *
                 * Next iterations will
                 * see FOLLOWER and stop
                 * sending heartbeats.
                 */
            }
        }

        std::this_thread::sleep_for(
            interval_
        );
    }
}

}