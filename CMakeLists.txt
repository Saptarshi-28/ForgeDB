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
		src/commands/CommandParser.cpp
		src/commands/CommandHandler.cpp
		src/storage/WAL.cpp
)

target_include_directories(forgedb
    	PRIVATE
    	include
)
