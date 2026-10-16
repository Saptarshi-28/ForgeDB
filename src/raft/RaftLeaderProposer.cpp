#include "raft/RaftLeaderProposer.h"

#include <sstream>
#include <stdexcept>
#include <string>

namespace forgedb::raft {

RaftLeaderProposer::RaftLeaderProposer(
    RaftState& state,
    RaftLog& log
)
    : state_(state),
      log_(log)
{
}

uint64_t RaftLeaderProposer::propose(
    const std::string& command
)
{
    std::lock_guard<std::mutex> lock(mutex_);

    /*
     * Validate the supported command grammar.
     * Keys and values are single tokens.
     */
    std::istringstream stream(command);

    std::string operation;
    std::string key;
    std::string value;
    std::string extra;

    stream >> operation;

    bool valid = false;

    if (operation == "SET") {
        valid =
            static_cast<bool>(stream >> key >> value) &&
            !(stream >> extra);
    }
    else if (operation == "DELETE") {
        valid =
            static_cast<bool>(stream >> key) &&
            !(stream >> extra);
    }

    if (!valid) {
        throw std::invalid_argument(
            "Invalid Raft mutation"
        );
    }

    /*
     * Only an elected leader may propose.
     */
    if (state_.role() != RaftRole::LEADER) {
        throw std::runtime_error(
            "Not Raft leader"
        );
    }

    const uint64_t term = state_.currentTerm();

    if (term == 0) {
        throw std::runtime_error(
            "Cannot propose in Raft term zero"
        );
    }

    /*
     * RaftLog::append() provides durable
     * log persistence before returning.
     *
     * It does NOT mean majority committed.
     */
    auto entry = log_.append(term, command);

    /*
     * Detect leadership changes that occurred
     * while the append was in progress.
     *
     * A failed check here means the entry
     * might still exist in the local log.
     */
    if (
        state_.role() != RaftRole::LEADER ||
        state_.currentTerm() != term
    ) {
        throw std::runtime_error(
            "Leadership changed; proposal outcome uncertain"
        );
    }

    return entry.index;
}

}