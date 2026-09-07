#pragma once
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include "storage/WAL.h"

namespace forgedb::storage {

	class KeyValueStore {

		public:
				KeyValueStore(const std::string& wal_filename);
				
    			void set(const std::string& key, const std::string& value);

    			std::string get(const std::string& key);

    			bool remove(const std::string& key);

		private:
    			std::unordered_map<std::string, std::string> data_;
				std::shared_mutex mutex_;
				WAL wal_;
				void applyOperation(const std::string& operation);
	};

}
