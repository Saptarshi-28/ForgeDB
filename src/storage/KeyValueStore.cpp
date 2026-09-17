#include "storage/KeyValueStore.h"
#include <sstream>
#include <shared_mutex>
#include <mutex>

namespace forgedb::storage {

	KeyValueStore::KeyValueStore(const std::string& wal_filename): wal_(wal_filename)
	{
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

    	    data_[key] = value;
    	}
    	else if (command == "DELETE") {
    	    stream >> key;

    	    data_.erase(key);
    	}
	}

	void KeyValueStore::set(const std::string& key,const std::string& value){
		std::unique_lock<std::shared_mutex> lock(mutex_);
    	wal_.append("SET " + key + " " + value);
		data_[key] = value;
	}

	std::string KeyValueStore::get(const std::string& key) {
		std::shared_lock<std::shared_mutex> lock(mutex_);
    	auto it = data_.find(key);
    	if (it == data_.end()) return "";
		return it->second;
	}

	bool KeyValueStore::remove(const std::string& key)
	{
	    std::unique_lock<std::shared_mutex> lock(mutex_);

	    auto it = data_.find(key);

	    if (it == data_.end()) {
	        return false;
	    }

	    wal_.append("DELETE " + key);
	    data_.erase(it);

	    return true;
	}
}
