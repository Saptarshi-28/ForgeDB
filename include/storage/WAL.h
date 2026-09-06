#pragma once
#include <string>
#include <vector>

namespace forgedb::storage {

    class WAL {
        public:
            WAL(const std::string& filename);
            void append(const std::string& operation);
            std::vector<std::string> replay();
        private:
            std::string filename_;
    };
}