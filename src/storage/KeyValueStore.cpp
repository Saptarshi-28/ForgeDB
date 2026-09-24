#include "storage/KeyValueStore.h"
#include <sstream>
#include <shared_mutex>
#include <mutex>
#include <filesystem>
#include <algorithm>
#include "storage/Compaction.h"

namespace forgedb::storage {

	KeyValueStore::KeyValueStore(const std::string& wal_filename): wal_(wal_filename)
	{
		initializeNextSSTableId();

		auto operations = wal_.replay();

		for (const auto& operation : operations) {
        	applyOperation(operation);
    	}
	}
	void KeyValueStore::applyOperation(const std::string& operation)
	{
	    std::istringstream stream(operation);

	    std::string command;
	    std::string key;
	    std::string value;

	    stream >> command;

	    if (command == "SET") {
	        stream >> key;
	        stream >> value;

	        memtable_.set(key, value);
	    }
	    else if (command == "DELETE") {
	        stream >> key;

	        memtable_.remove(key);
	    }
	}

	void KeyValueStore::set(const std::string& key, const std::string& value)
	{
	    std::unique_lock<std::shared_mutex> lock(mutex_);

	    wal_.append("SET " + key + " " + value);
	    memtable_.set(key, value);

		if (memtable_.shouldFlush(MEMTABLE_MAX_ENTRIES)) {
		    flushMemTable();
		}
	}

	std::string KeyValueStore::get(const std::string& key)
	{
	    std::shared_lock<std::shared_mutex> lock(mutex_);

	    if (memtable_.contains(key)) {
	        return memtable_.get(key);
	    }

	    if (memtable_.isDeleted(key)) {
	        return "";
	    }

	    auto tables = listSSTables();

	    for (auto it = tables.rbegin(); it != tables.rend(); ++it) {

	        auto result = SSTable::lookup(*it, key);

	        if (!result.has_value()) {
	            continue;
	        }

	        if (result->deleted) {
	            return "";
	        }

	        return result->value;
	    }

	    return "";
	}

	bool KeyValueStore::remove(const std::string& key)
	{
	    std::unique_lock<std::shared_mutex> lock(mutex_);

	    bool exists = false;

	    if (memtable_.contains(key)) {
	        exists = true;
	    }
	    else if (memtable_.isDeleted(key)) {
	        return false;
	    }
	    else {
	        auto tables = listSSTables();

	        for (auto it = tables.rbegin(); it != tables.rend(); ++it) {

	            auto result = SSTable::lookup(*it, key);

	            if (!result.has_value()) {
	                continue;
	            }

	            if (result->deleted) {
	                return false;
	            }

	            exists = true;
	            break;
	        }
	    }

	    if (!exists) {
	        return false;
	    }

	    wal_.append("DELETE " + key);
	    memtable_.remove(key);

	    if (memtable_.shouldFlush(MEMTABLE_MAX_ENTRIES)) {
	        flushMemTable();
	    }

	    return true;
	}

	std::string KeyValueStore::nextSSTableFilename()
	{
	    return "sstable_" + std::to_string(next_sstable_id_++) + ".db";
	}
	void KeyValueStore::initializeNextSSTableId()
	{
	    namespace fs = std::filesystem;

	    std::size_t max_id = 0;
	    bool found_sstable = false;

	    for (const auto& entry : fs::directory_iterator(".")) {

	        if (!entry.is_regular_file()) {
	            continue;
	        }

	        std::string filename = entry.path().filename().string();

	        const std::string prefix = "sstable_";
	        const std::string suffix = ".db";

	        if (filename.starts_with(prefix) &&
	            filename.ends_with(suffix)) {

	            std::string id_part = filename.substr(
	                prefix.size(),
	                filename.size() - prefix.size() - suffix.size()
	            );

	            try {
	                std::size_t id = std::stoull(id_part);

	                if (!found_sstable || id > max_id) {
	                    max_id = id;
	                    found_sstable = true;
	                }
	            }
	            catch (...) {
	                // Ignore files that do not contain a valid numeric ID.
	            }
	        }
	    }

	    next_sstable_id_ = found_sstable ? max_id + 1 : 0;
	}

	void KeyValueStore::flushMemTable()
	{
	    auto entries = memtable_.snapshot();

	    SSTable::write(
	        nextSSTableFilename(),
	        entries
	    );
		wal_.reset();
	    memtable_.clear();
		maybeCompact();
	}

	std::vector<std::string> KeyValueStore::listSSTables() const
	{
	    namespace fs = std::filesystem;

	    std::vector<std::pair<std::size_t, std::string>> tables;

	    const std::string prefix = "sstable_";
	    const std::string suffix = ".db";

	    for (const auto& entry : fs::directory_iterator(".")) {

	        if (!entry.is_regular_file()) {
	            continue;
	        }

	        std::string filename =
	            entry.path().filename().string();

	        if (!filename.starts_with(prefix) ||
	            !filename.ends_with(suffix)) {
	            continue;
	        }

	        std::string id_part = filename.substr(
	            prefix.size(),
	            filename.size() - prefix.size() - suffix.size()
	        );

	        try {
	            std::size_t id = std::stoull(id_part);

	            tables.emplace_back(
	                id,
	                filename
	            );
	        }
	        catch (...) {
	            // Ignore malformed SSTable filenames.
	        }
	    }

	    std::sort(
	        tables.begin(),
	        tables.end(),
	        [](const auto& a, const auto& b) {
	            return a.first < b.first;
	        }
	    );

	    std::vector<std::string> filenames;

	    filenames.reserve(tables.size());

	    for (const auto& [id, filename] : tables) {
	        filenames.push_back(filename);
	    }

	    return filenames;
	}
	void KeyValueStore::maybeCompact()
	{
	    auto tables = listSSTables();

	    if (tables.size() < 4) {
	        return;
	    }

	    std::string output_file = nextSSTableFilename();

	    Compaction::compact(
	        tables,
	        output_file
	    );
	}
}
