#pragma once
#include <mutex>
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

    void appendWithoutSync(const std::string& operation);
    void sync();

private:
    std::string filename_;
    int fd_;
    std::mutex mutex_;
};

}