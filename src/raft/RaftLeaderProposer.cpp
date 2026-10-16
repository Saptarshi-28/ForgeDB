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
     * Leadership validation and durable append
     * are now atomic with respect to RaftState
     * leadership transitions.
     */
    return state_.appendCommandIfLeader(
        log_,
        command
    );
}

}