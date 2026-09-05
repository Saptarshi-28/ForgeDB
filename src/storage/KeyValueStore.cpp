#include "storage/KeyValueStore.h"

namespace forgedb::storage {

	void KeyValueStore::set(const std::string& key,const std::string& value){
    		data_[key] = value;
	}

	std::string KeyValueStore::get(const std::string& key) {
    		auto it = data_.find(key);

    		if (it == data_.end()) {
        		return "";
    		}

    		return it->second;
	}

	bool KeyValueStore::remove(const std::string& key){
    		return data_.erase(key) > 0;
	}

}
