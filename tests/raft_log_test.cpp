#include "raft/RaftLog.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

int main()
{
    using namespace forgedb::raft;

    const std::string filename =
        "build/raft_log_test.log";

    std::filesystem::remove(filename);

    // Test 1: Fresh log and durable appends.
    {
        RaftLog log(filename);

        auto [index, term] =
            log.lastLogInfo();

        assert(index == 0);
        assert(term == 0);

        auto first =
            log.append(1, "SET alpha one");

        auto second =
            log.append(1, "SET beta two");

        auto third =
            log.append(2, "DELETE alpha");

        assert(first.index == 1);
        assert(second.index == 2);
        assert(third.index == 3);

        assert(first.term == 1);
        assert(second.term == 1);
        assert(third.term == 2);

        std::cout
            << "Initial append passed\n";
    }

    // Test 2: Simulate restarting the node.
    {
        RaftLog restarted(filename);

        auto [index, term] =
            restarted.lastLogInfo();

        assert(index == 3);
        assert(term == 2);

        auto entries =
            restarted.load();

        assert(entries.size() == 3);

        auto fourth =
            restarted.append(
                3,
                "SET gamma three"
            );

        assert(fourth.index == 4);
        assert(fourth.term == 3);

        std::cout
            << "Restart recovery passed\n";
    }

    // Test 3: Reject decreasing terms.
    {
        RaftLog log(filename);

        bool rejected = false;

        try {
            log.append(
                2,
                "SET invalid value"
            );
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }

        assert(rejected);

        std::cout
            << "Term validation passed\n";
    }

    /*
     * Test 4:
     * Simulate a crash during an append.
     *
     * The next entry would have been 5,
     * but the final record is incomplete.
     */
    {
        std::ofstream file(
            filename,
            std::ios::binary | std::ios::app
        );

        file << "5\t3\tSET broken";
    }

    // Test 5: Recover and repair torn tail.
    {
        RaftLog recovered(filename);

        auto [index, term] =
            recovered.lastLogInfo();

        assert(index == 4);
        assert(term == 3);

        auto fifth =
            recovered.append(
                3,
                "SET recovered yes"
            );

        assert(fifth.index == 5);
        assert(fifth.term == 3);

        auto entries =
            recovered.load();

        assert(entries.size() == 5);

        assert(
            entries.back().command ==
            "SET recovered yes"
        );

        std::cout
            << "Truncated-record recovery passed\n";
    }

    {
        RaftLog log(filename);

        auto term0 =
            log.termAt(0);

        assert(
            term0.has_value()
        );

        assert(
            *term0 == 0
        );

        auto term1 =
            log.termAt(1);

        assert(
            term1.has_value()
        );

        assert(
            *term1 == 1
        );

        auto term3 =
            log.termAt(3);

        assert(
            term3.has_value()
        );

        assert(
            *term3 == 2
        );

        auto term5 =
            log.termAt(5);

        assert(
            term5.has_value()
        );

        assert(
            *term5 == 3
        );

        auto missing =
            log.termAt(999);

        assert(
            !missing.has_value()
        );

        std::cout
            << "Log term lookup passed\n";
    }

    std::cout
        << "All Raft log tests passed\n";

    return 0;
}