#pragma once
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include "storage/WAL.h"
#include "storage/MemTable.h"
#include "storage/SSTable.h"
#include <cstddef>
#include <vector>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>

namespace forgedb::storage {

	class KeyValueStore {

		public:
			KeyValueStore(const std::string& wal_filename);
			~KeyValueStore();

    		void set(const std::string& key, const std::string& value);

    		std::string get(const std::string& key);

    		bool remove(const std::string& key);

		private:
			enum class WriteType {
			    SET,
			    DELETE
			};

			struct PendingWrite {
			    WriteType type;
			    std::string key;
			    std::string value;

			    std::promise<bool> completion;
			};

    		MemTable memtable_;
			std::size_t next_sstable_id_ = 0;
			std::shared_mutex mutex_;
			WAL wal_;

			std::condition_variable write_queue_cv_;
			std::thread writer_thread_;
			bool stopping_ = false;

			static constexpr std::size_t MEMTABLE_MAX_ENTRIES = 1000;
			std::mutex write_queue_mutex_;

			std::deque<std::shared_ptr<PendingWrite>> pending_writes_;

			void writerLoop();
			std::vector<std::string> listSSTables() const;
			void flushMemTable();
			std::string nextSSTableFilename();
			void initializeNextSSTableId();
			void applyOperation(const std::string& operation);
			void maybeCompact();

			bool submitWrite(WriteType type,const std::string& key,const std::string& value);
			void processWriteBatches();

	};

}
