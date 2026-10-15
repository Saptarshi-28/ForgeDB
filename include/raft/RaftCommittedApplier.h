#pragma once

#include "raft/RaftLog.h"
#include "raft/RaftCommitState.h"
#include "raft/RaftApplyProgress.h"
#include "storage/KeyValueStore.h"

#include <cstddef>
#include <mutex>

namespace forgedb::raft {

class RaftCommittedApplier {
public:
    RaftCommittedApplier(
        RaftLog& log,
        RaftCommitState& commit,
        RaftApplyProgress& progress,
        forgedb::storage::KeyValueStore& store
    );

    // Returns the number of entries applied.
    std::size_t applyCommitted();

private:
    RaftLog& log_;
    RaftCommitState& commit_;
    RaftApplyProgress& progress_;
    forgedb::storage::KeyValueStore& store_;

    // Only one application loop at a time.
    std::mutex mutex_;
};

}