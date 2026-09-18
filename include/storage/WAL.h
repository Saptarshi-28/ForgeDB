#pragma once

#include <string>
#include <vector>

namespace forgedb::storage {

class WAL {
public:
    WAL(const std::string& filename);
    ~WAL();

    void append(const std::string& operation);
    std::vector<std::string> replay();
    void reset();

private:
    std::string filename_;
    int fd_;
};

}