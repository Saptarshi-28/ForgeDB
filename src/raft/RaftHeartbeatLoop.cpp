#include "raft/RaftHeartbeatLoop.h"
#include "raft/RaftHeartbeat.h"
#include <iostream>
#include <stdexcept>
#include <thread>
#include <utility>

namespace forgedb::raft {

RaftHeartbeatLoop::RaftHeartbeatLoop(
    const std::string& node_id,
    RaftState& state,
    RaftLog& log,
    const std::vector<RaftPeer>& peers,
    RaftLeaderProgress& progress,
    RaftCommitState& commit_state,
    std::chrono::milliseconds interval,
    std::function<void()> on_commit_advanced
)
    : node_id_(node_id),
      state_(state),
      log_(log),
      peers_(peers),
      progress_(progress),
      commit_state_(commit_state),
      interval_(interval),
      on_commit_advanced_(
          std::move(on_commit_advanced)
      )
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
        peers_,
        progress_
    );
    uint64_t initialized_leader_term = 0;
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
            const uint64_t current_term = state_.currentTerm();

            if (
                initialized_leader_term !=
                current_term
            ) {
                const auto last_log =
                    log_.lastLogInfo();

                progress_.reset(
                    last_log.first
                );

                initialized_leader_term =
                    current_term;

                std::cout
                    << node_id_
                    << " initialized follower progress "
                    << "for leader term "
                    << current_term
                    << " at nextIndex "
                    << last_log.first + 1
                    << "\n";
            }
            /*
             * Read the current committed index.
             *
             * This will be included in every
             * AppendEntries RPC during this round.
             */
            const uint64_t leader_commit =
                commit_state_.commitIndex();

            auto result =
                heartbeat.sendHeartbeats(
                    leader_commit
                );

            /*
             * Do not calculate leader commit
             * if this node stepped down while
             * communicating with followers.
             */
            if (
                result.stepped_down ||
                state_.role() != RaftRole::LEADER ||
                state_.currentTerm() != current_term
            ) {
                continue;
            }

            /*
             * Take a thread-safe snapshot
             * of follower matchIndex values.
             */
            auto follower_indexes =
                progress_.followerMatchIndexes();

            const uint64_t old_commit =
                commit_state_.commitIndex();

            /*
             * Calculate the highest index
             * replicated on a majority.
             *
             * Only current-term entries may
             * directly advance commitIndex.
             */
            const uint64_t new_commit =
                commit_state_.advanceLeaderCommit(
                    current_term,
                    follower_indexes,
                    log_
                );

            if (new_commit > old_commit) {
                std::cout
                    << node_id_
                    << " advanced commitIndex "
                    << old_commit
                    << " -> "
                    << new_commit
                    << " in term "
                    << current_term
                    << "\n";

                /*
                 * RaftCommitState has already
                 * persisted the committed prefix.
                 *
                 * Now apply the committed commands.
                 */
                if (on_commit_advanced_) {
                    on_commit_advanced_();
                }
            }
        }

        std::this_thread::sleep_for(
            interval_
        );
    }
}

}