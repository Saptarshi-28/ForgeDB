#include "raft/RaftClient.h"
#include "raft/RaftMessages.h"

#include <cassert>
#include <iostream>

int main()
{
    using namespace forgedb::raft;

    RaftClient client;

    /*
     * -----------------------------------
     * Test 1:
     * Replicate the first real entry.
     * -----------------------------------
     */
    AppendEntriesRequest first{
        1000,
        "node1",
        0,
        0,
        0,
        {
            {
                1,
                1000,
                "SET alpha one"
            }
        }
    };

    auto first_response =
        client.appendEntries(
            "127.0.0.1",
            6380,
            first
        );

    assert(
        first_response.has_value()
    );

    assert(
        first_response->success
    );

    assert(
        first_response->term ==
        1000
    );

    std::cout
        << "First entry replication passed\n";

    /*
     * -----------------------------------
     * Test 2:
     * Append another entry using
     * index 1 as the matching prefix.
     * -----------------------------------
     */
    AppendEntriesRequest second{
        1000,
        "node1",
        1,
        1000,
        0,
        {
            {
                2,
                1000,
                "SET beta two"
            }
        }
    };

    auto second_response =
        client.appendEntries(
            "127.0.0.1",
            6380,
            second
        );

    assert(
        second_response.has_value()
    );

    assert(
        second_response->success
    );

    std::cout
        << "Second entry replication passed\n";

    /*
     * -----------------------------------
     * Test 3:
     * Retry the exact same entry.
     *
     * This must be idempotent.
     * -----------------------------------
     */
    auto duplicate_response =
        client.appendEntries(
            "127.0.0.1",
            6380,
            second
        );

    assert(
        duplicate_response.has_value()
    );

    assert(
        duplicate_response->success
    );

    std::cout
        << "Duplicate replication passed\n";

    /*
     * -----------------------------------
     * Test 4:
     * New leader/term replaces a stale
     * conflicting suffix.
     *
     * Keep index 1.
     * Replace index 2.
     * Add index 3.
     * -----------------------------------
     */
    AppendEntriesRequest replacement{
        1001,
        "node1",
        1,
        1000,
        0,
        {
            {
                2,
                1001,
                "SET beta replaced"
            },
            {
                3,
                1001,
                "SET gamma three"
            }
        }
    };

    auto replacement_response =
        client.appendEntries(
            "127.0.0.1",
            6380,
            replacement
        );

    assert(
        replacement_response.has_value()
    );

    assert(
        replacement_response->success
    );

    assert(
        replacement_response->term ==
        1001
    );

    std::cout
        << "Network conflict repair passed\n";

    std::cout
        << "All Raft replication RPC tests passed\n";

    return 0;
}