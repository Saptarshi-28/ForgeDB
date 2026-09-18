#include "storage/KeyValueStore.h"
#include <sstream>
#include <shared_mutex>
#include <mutex>
#include <filesystem>

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

	    for (std::size_t id = next_sstable_id_; id > 0; --id) {

	        std::size_t current_id = id - 1;

	        std::string filename =
	            "sstable_" + std::to_string(current_id) + ".db";

	        auto result = SSTable::lookup(filename, key);

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
	        for (std::size_t id = next_sstable_id_; id > 0; --id) {

	            std::size_t current_id = id - 1;

	            std::string filename =
	                "sstable_" + std::to_string(current_id) + ".db";

	            auto result = SSTable::lookup(filename, key);

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

	    if (memtable_.size() == MEMTABLE_MAX_ENTRIES) {
	        auto entries = memtable_.snapshot();

	        SSTable::write(
	            nextSSTableFilename(),
	            entries
	        );
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
	}
}
