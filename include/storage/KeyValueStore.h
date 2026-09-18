#pragma once
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include "storage/WAL.h"
#include "storage/MemTable.h"
#include "storage/SSTable.h"
#include <cstddef>

namespace forgedb::storage {

	class KeyValueStore {

		public:
				KeyValueStore(const std::string& wal_filename);

    			void set(const std::string& key, const std::string& value);

    			std::string get(const std::string& key);

    			bool remove(const std::string& key);

		private:
    MemTable memtable_;
				std::size_t next_sstable_id_ = 0;

				std::shared_mutex mutex_;
				WAL wal_;
				static constexpr std::size_t MEMTABLE_MAX_ENTRIES = 1000;

				void flushMemTable();
				std::string nextSSTableFilename();
				void initializeNextSSTableId();
				void applyOperation(const std::string& operation);
	};

}
