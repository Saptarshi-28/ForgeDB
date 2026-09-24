#pragma once

#include <string>
#include <vector>

namespace forgedb::storage {

class Compaction {
public:
    static void compact(
        const std::vector<std::string>& input_files,
        const std::string& output_file
    );
};

}