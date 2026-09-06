#include "storage/WAL.h"
#include <fstream>
#include <iostream>

namespace forgedb::storage{

    WAL::WAL(const std::string& filename) : filename_(filename) {}

    void WAL::append(const std::string& operation)
    {
        std::ofstream wal_file(filename_, std::ios::app);

        if (wal_file.is_open()) {
            wal_file << operation << std::endl;
            wal_file.close();
        }
        else {
            std::cerr << "Failed to open WAL file for writing: "<< filename_ << std::endl;
        }
    }

    std::vector<std::string> WAL::replay()
    {
        std::vector<std::string> operations;

        std::ifstream wal_file(filename_);

        if (!wal_file.is_open()) return operations;
    
        std::string operation;

        while (std::getline(wal_file, operation)) {
            if (!operation.empty()) operations.push_back(operation);
        }
    

        wal_file.close();

        return operations;
    }
}