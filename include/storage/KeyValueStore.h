#pragma once

#include <string>
#include <unordered_map>

namespace forgedb::storage {

	class KeyValueStore {
		public:
    			void set(const std::string& key, const std::string& value);

    			std::string get(const std::string& key);

    			bool remove(const std::string& key);

		private:
    			std::unordered_map<std::string, std::string> data_;
	};

}
