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

        /*
        * Full entry lookup.
        */
        auto entry1 =
            log.entryAt(1);

        assert(
            entry1.has_value()
        );

        assert(
            entry1->index == 1
        );

        assert(
            entry1->term == 1
        );

        assert(
            entry1->command ==
            "SET alpha one"
        );

        assert(
            !log.entryAt(0).has_value()
        );

        assert(
            !log.entryAt(999).has_value()
        );

        /*
         * Log suffix lookup.
         */
        auto suffix =
            log.entriesFrom(2);

        assert(
            suffix.size() == 4
        );

        assert(
            suffix[0].index == 2
        );

        assert(
            suffix[0].term == 1
        );

        assert(
            suffix[0].command ==
            "SET beta two"
        );

        assert(
            suffix[1].index == 3
        );

        assert(
            suffix[2].index == 4
        );

        assert(
            suffix[3].index == 5
        );

        for (
            std::size_t i = 1;
            i < suffix.size();
            ++i
        ) {
            assert(
                suffix[i].index ==
                suffix[i - 1].index + 1
            );
        }

        auto all_entries =
            log.entriesFrom(1);

        assert(
            all_entries.size() == 5
        );

        assert(
            all_entries.front().index == 1
        );

        assert(
            all_entries.back().index == 5
        );

        auto missing_suffix =
            log.entriesFrom(999);

        assert(
            missing_suffix.empty()
        );

        std::cout
            << "Log entry and suffix lookup passed\n";

    }

    /*
     * Test 7:
     * Durable suffix truncation.
     */
    {
        RaftLog log(filename);

        auto before =
            log.load();

        assert(
            before.size() == 5
        );

        log.truncateSuffix(4);

        auto after =
            log.load();

        assert(
            after.size() == 3
        );

        assert(
            after[0].index == 1
        );

        assert(
            after[1].index == 2
        );

        assert(
            after[2].index == 3
        );

        assert(
            !log.entryAt(4).has_value()
        );

        assert(
            !log.entryAt(5).has_value()
        );

        auto [
            last_index,
            last_term
        ] =
            log.lastLogInfo();

        assert(
            last_index == 3
        );

        assert(
            last_term == 2
        );

        std::cout
            << "Suffix truncation passed\n";
    }

    /*
     * Test 8:
     * Truncation survives restart.
     */
    {
        RaftLog restarted(filename);

        auto entries =
            restarted.load();

        assert(
            entries.size() == 3
        );

        auto [
            last_index,
            last_term
        ] =
            restarted.lastLogInfo();

        assert(
            last_index == 3
        );

        assert(
            last_term == 2
        );

        auto replacement =
            restarted.append(
                3,
                "SET replacement yes"
            );

        assert(
            replacement.index == 4
        );

        assert(
            replacement.term == 3
        );

        std::cout
            << "Truncation restart recovery passed\n";
    }
    /*
     * Truncating beyond the end should
     * change nothing.
     */
    {
        RaftLog log(filename);

        auto before =
            log.load();

        log.truncateSuffix(
            999
        );

        auto after =
            log.load();

        assert(
            before.size() ==
            after.size()
        );
        assert(
            before.back().index ==
            after.back().index
        );
        assert(
            before.back().term ==
            after.back().term
        );
        assert(
            before.back().command ==
            after.back().command
        );
        std::cout<< "Out-of-range truncation no-op passed\n";
    }
    /*
     * Index zero is the Raft sentinel
     * and cannot be truncated.
     */
    {
        RaftLog log(filename);

        bool rejected = false;

        try {
            log.truncateSuffix(0);
        }
        catch (
            const std::runtime_error&
        ) {
            rejected = true;
        }

        assert(rejected);

        std::cout
            << "Invalid truncation index rejected\n";
    }
    std::cout
        << "All Raft log tests passed\n";

    return 0;
}