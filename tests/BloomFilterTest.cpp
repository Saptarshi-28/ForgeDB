#include "storage/BloomFilter.h"

#include <filesystem>
#include <iostream>

int main()
{
    using namespace forgedb::storage;

    const std::string filename = "bloom_filter_test.bf";

    {
        BloomFilter filter(
            10000,
            3
        );

        filter.add("apple");
        filter.add("banana");
        filter.add("orange");

        filter.save(filename);
    }

    BloomFilter loaded =
        BloomFilter::load(filename);

    std::cout << std::boolalpha;

    std::cout
        << "apple: "
        << loaded.possiblyContains("apple")
        << '\n';

    std::cout
        << "banana: "
        << loaded.possiblyContains("banana")
        << '\n';

    std::cout
        << "orange: "
        << loaded.possiblyContains("orange")
        << '\n';

    std::cout
        << "grape: "
        << loaded.possiblyContains("grape")
        << '\n';

    std::filesystem::remove(filename);

    return 0;
}