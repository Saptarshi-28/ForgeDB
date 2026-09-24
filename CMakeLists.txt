cmake_minimum_required(VERSION 3.20)

project(ForgeDB
        VERSION 0.1.0
        LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(forgedb
        src/main.cpp
        src/network/Server.cpp
        src/storage/KeyValueStore.cpp
        src/storage/WAL.cpp
        src/storage/MemTable.cpp
        src/storage/SSTable.cpp
        src/storage/Compaction.cpp
        src/commands/CommandParser.cpp
        src/commands/CommandHandler.cpp
        src/concurrency/ThreadPool.cpp
)

add_executable(forge_benchmark
        benchmarks/Benchmark.cpp
)

target_include_directories(forgedb
        PRIVATE
        include
)

add_executable(thread_pool_test
        tests/ThreadPoolTest.cpp
        src/concurrency/ThreadPool.cpp
)

target_include_directories(thread_pool_test
        PRIVATE
        include
)

add_executable(sstable_test
        tests/SSTableTest.cpp
        src/storage/SSTable.cpp
)

target_include_directories(sstable_test PRIVATE include)

add_executable(compaction_test
        tests/CompactionTest.cpp
        src/storage/SSTable.cpp
        src/storage/Compaction.cpp
)

target_include_directories(compaction_test PRIVATE include)