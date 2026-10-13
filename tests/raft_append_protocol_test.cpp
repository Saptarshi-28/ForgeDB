#include "raft/RaftProtocol.h"

#include <cassert>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    /*
     * ------------------------------------------------
     * 1. Heartbeat: zero entries
     * ------------------------------------------------
     */
    AppendEntriesRequest heartbeat{
        5,
        "node1",
        0,
        0,
        0,
        {}
    };

    std::string heartbeat_wire =
        encodeAppendEntries(
            heartbeat
        );

    std::cout
        << "Heartbeat wire:\n"
        << heartbeat_wire;

    auto decoded_heartbeat =
        decodeAppendEntries(
            heartbeat_wire
        );

    assert(
        decoded_heartbeat.has_value()
    );

    assert(
        decoded_heartbeat->term == 5
    );

    assert(
        decoded_heartbeat->leader_id ==
        "node1"
    );

    assert(
        decoded_heartbeat->
            prev_log_index == 0
    );

    assert(
        decoded_heartbeat->
            prev_log_term == 0
    );

    assert(
        decoded_heartbeat->
            leader_commit == 0
    );

    assert(
        decoded_heartbeat->
            entries.empty()
    );

    /*
     * ------------------------------------------------
     * 2. AppendEntries with real commands
     * ------------------------------------------------
     */
    AppendEntriesRequest replication{
        6,
        "node2",
        7,
        5,
        6,
        {
            {
                8,
                6,
                "SET alpha one"
            },
            {
                9,
                6,
                "DELETE beta"
            }
        }
    };

    std::string replication_wire =
        encodeAppendEntries(
            replication
        );

    std::cout
        << "\nReplication wire:\n"
        << replication_wire;

    auto decoded =
        decodeAppendEntries(
            replication_wire
        );

    assert(
        decoded.has_value()
    );

    assert(
        decoded->term == 6
    );

    assert(
        decoded->leader_id ==
        "node2"
    );

    assert(
        decoded->prev_log_index == 7
    );

    assert(
        decoded->prev_log_term == 5
    );

    assert(
        decoded->leader_commit == 6
    );

    assert(
        decoded->entries.size() == 2
    );

    assert(
        decoded->entries[0].index == 8
    );

    assert(
        decoded->entries[0].term == 6
    );

    assert(
        decoded->entries[0].command ==
        "SET alpha one"
    );

    assert(
        decoded->entries[1].index == 9
    );

    assert(
        decoded->entries[1].term == 6
    );

    assert(
        decoded->entries[1].command ==
        "DELETE beta"
    );

    /*
     * ------------------------------------------------
     * 3. Reject broken hex
     * ------------------------------------------------
     */
    assert(
        !decodeAppendEntries(
            "RAFT_APPEND "
            "1 node1 0 0 0 "
            "1 1 1 XYZ\n"
        ).has_value()
    );

    /*
     * ------------------------------------------------
     * 4. Reject missing entry
     * ------------------------------------------------
     */
    assert(
        !decodeAppendEntries(
            "RAFT_APPEND "
            "1 node1 0 0 0 1\n"
        ).has_value()
    );

    /*
     * ------------------------------------------------
     * 5. Reject non-contiguous index
     * ------------------------------------------------
     *
     * prev index = 5, so the first
     * new entry must be index 6.
     */
    assert(
        !decodeAppendEntries(
            "RAFT_APPEND "
            "2 node1 5 1 0 "
            "1 8 2 "
            "53455420612062\n"
        ).has_value()
    );

    /*
     * ------------------------------------------------
     * 6. Reject trailing garbage
     * ------------------------------------------------
     */
    assert(
        !decodeAppendEntries(
            "RAFT_APPEND "
            "1 node1 0 0 0 "
            "0 unexpected\n"
        ).has_value()
    );

    std::cout
        << "\nAppendEntries protocol tests passed\n";

    return 0;
}