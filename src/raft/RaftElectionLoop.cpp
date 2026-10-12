#include "raft/RaftElectionLoop.h"
#include "raft/RaftElection.h"

#include <iostream>
#include <stdexcept>
#include <thread>

namespace forgedb::raft {

RaftElectionLoop::RaftElectionLoop(
    const std::string& node_id,
    RaftState& state,
    RaftLog& log,
    const std::vector<RaftPeer>& peers,
    ElectionTimer& election_timer,
    std::chrono::milliseconds poll_interval
)
    : node_id_(node_id),
      state_(state),
      log_(log),
      peers_(peers),
      election_timer_(election_timer),
      poll_interval_(poll_interval)
{
    if (node_id_.empty()) {
        throw std::invalid_argument(
            "Raft node ID cannot be empty"
        );
    }

    if (
        poll_interval_.count() <= 0
    ) {
        throw std::invalid_argument(
            "Invalid election poll interval"
        );
    }
}

RaftElectionLoop::~RaftElectionLoop()
{
    stop();
}

void RaftElectionLoop::start()
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
            &RaftElectionLoop::run,
            this
        );
}

void RaftElectionLoop::stop()
{
    running_.store(false);

    if (
        worker_.joinable()
    ) {
        worker_.join();
    }
}

bool RaftElectionLoop::running() const
{
    return running_.load();
}

void RaftElectionLoop::run()
{
    bool was_leader = false;
    RaftElection election(
        node_id_,
        state_,
        log_,
        peers_
    );

    while (
        running_.load()
    ) {
        auto current_role = state_.role();

        if (
            current_role ==
            RaftRole::LEADER
        ) {
            was_leader = true;

            std::this_thread::sleep_for(
                poll_interval_
            );

            continue;
        }

        /*
         * If this node was leader but has
         * stepped down, give the new leader
         * a full election timeout window.
         */
        if (was_leader) {

            election_timer_.reset();

            was_leader = false;

            std::cout
                << node_id_
                << " stepped down; "
                << "election deadline reset\n";
        }

        if (
            !election_timer_.expired()
        ) {
            std::this_thread::sleep_for(
                poll_interval_
            );

            continue;
        }

        std::cout
            << node_id_
            << " election timeout expired\n";

        try {

            ElectionResult result =
                election.runElection();

            if (result.won) {

                std::cout
                    << node_id_
                    << " became Raft leader "
                    << "for term "
                    << result.term
                    << "\n";

                /*
                 * Do not reset our election
                 * timer while leader.
                 *
                 * The loop ignores it until
                 * this node becomes follower
                 * again.
                 */
            }
            else {

                /*
                 * Election did not obtain a
                 * majority, or we discovered
                 * a newer term.
                 *
                 * Wait for another randomized
                 * timeout before retrying.
                 */
                election_timer_.reset();

                std::cout
                    << node_id_
                    << " election failed; "
                    << "new deadline created\n";
            }
        }
        catch (
            const std::exception& e
        ) {

            std::cerr
                << "Raft election failed: "
                << e.what()
                << "\n";

            election_timer_.reset();
        }

        std::this_thread::sleep_for(
            poll_interval_
        );
    }
}

}