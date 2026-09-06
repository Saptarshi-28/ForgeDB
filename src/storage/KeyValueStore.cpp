#include "storage/KeyValueStore.h"
#include <sstream>

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
    	wal_.append("SET " + key + " " + value);
		data_[key] = value;
	}

	std::string KeyValueStore::get(const std::string& key) {
    	auto it = data_.find(key);
    	if (it == data_.end()) return "";
		return it->second;
	}

	bool KeyValueStore::remove(const std::string& key){
		bool removed = data_.erase(key) > 0;
    	if (removed) wal_.append("DELETE " + key);
    	return removed;
	}

}
