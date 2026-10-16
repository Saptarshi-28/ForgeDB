#include "raft/RaftCommittedApplier.h"
#include "commands/CommandParser.h"

#include <sstream>
#include <stdexcept>
#include <string>

namespace forgedb::raft {

RaftCommittedApplier::RaftCommittedApplier(
    RaftLog& log,
    RaftCommitState& commit,
    RaftApplyProgress& progress,
    forgedb::storage::KeyValueStore& store
)
    : log_(log),
      commit_(commit),
      progress_(progress),
      store_(store)
{
}

std::size_t
RaftCommittedApplier::applyCommitted()
{
    std::lock_guard<std::mutex> lock(mutex_);

    std::size_t applied_count = 0;

    forgedb::commands::CommandParser parser;

    while (true) {
        const uint64_t committed =
            commit_.commitIndex();

        const uint64_t applied =
            progress_.lastApplied();

        if (applied > committed) {
            throw std::runtime_error(
                "lastApplied exceeds commitIndex"
            );
        }

        if (committed > log_.protectedIndex()) {
            throw std::runtime_error(
                "Commit index is not durably protected"
            );
        }

        /*
         * Nothing else to apply.
         */
        if (applied == committed) {
            return applied_count;
        }

        /*
         * Process exactly the next entry.
         */
        const uint64_t next_index = applied + 1;

        auto entry = log_.entryAt(next_index);

        if (!entry.has_value()) {
            throw std::runtime_error(
                "Committed Raft log entry missing"
            );
        }
        /*
         * Internal Raft no-op entry.
         *
         * It changes no application data, but
         * still advances the durable applied index.
         *
         * Only the exact reserved command is valid.
         */
        if (entry->command == "RAFT_NOOP") {
            progress_.recordApplied(next_index);
            ++applied_count;
            continue;
        }
        auto command =
            parser.parse(entry->command);

        /*
         * The existing CommandParser does
         * not reject missing/extra arguments.
         *
         * Validate the exact supported
         * command grammar before execution.
         */
        std::istringstream stream(
            entry->command
        );

        std::string operation;
        std::string key;
        std::string value;
        std::string extra;

        using forgedb::commands::CommandType;

        if (command.type == CommandType::SET) {

            if (
                !(stream >> operation >> key >> value) ||
                (stream >> extra) ||
                operation != "SET" ||
                key != command.key ||
                value != command.value
            ) {
                throw std::runtime_error(
                    "Invalid committed SET command"
                );
            }

            /*
             * KeyValueStore::set() waits for
             * its background writer to finish.
             *
             * WAL is synced before success.
             */
            store_.set(
                command.key,
                command.value
            );
        }
        else if (
            command.type == CommandType::DELETE
        ) {

            if (
                !(stream >> operation >> key) ||
                (stream >> extra) ||
                operation != "DELETE" ||
                key != command.key
            ) {
                throw std::runtime_error(
                    "Invalid committed DELETE command"
                );
            }

            /*
             * DELETE must be applied even if
             * the key does not exist.
             *
             * A false return means "not found",
             * not a durability failure.
             */
            (void)store_.remove(
                command.key
            );
        }
        else {
            throw std::runtime_error(
                "Unsupported committed Raft command"
            );
        }

        /*
         * CRITICAL ORDER:
         *
         * 1. Validate command
         * 2. Durably apply to KeyValueStore
         * 3. Persist lastApplied
         *
         * If step 2 throws, step 3 is skipped.
         *
         * If step 3 fails after the KV write,
         * recovery may replay that same
         * idempotent SET/DELETE.
         */
        progress_.recordApplied(
            next_index
        );

        ++applied_count;
    }
}

}