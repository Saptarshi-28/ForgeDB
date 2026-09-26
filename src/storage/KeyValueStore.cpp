#include "storage/KeyValueStore.h"
#include <sstream>
#include <shared_mutex>
#include <mutex>
#include <filesystem>
#include <algorithm>
#include "storage/Compaction.h"
#include <exception>

namespace forgedb::storage {

	KeyValueStore::~KeyValueStore()
	{
	    {
	        std::lock_guard<std::mutex> lock(
	            write_queue_mutex_
	        );

	        stopping_ = true;
	    }

	    write_queue_cv_.notify_all();

	    if (writer_thread_.joinable()) {
	        writer_thread_.join();
	    }
	}
	KeyValueStore::KeyValueStore(const std::string& wal_filename): wal_(wal_filename)
	{
		initializeNextSSTableId();

		auto operations = wal_.replay();

		for (const auto& operation : operations) {
        	applyOperation(operation);
    	}
		writer_thread_ = std::thread(
		    &KeyValueStore::writerLoop,
		    this
		);
	}

	void KeyValueStore::writerLoop()
	{
	    while (true) {

	        {
	            std::unique_lock<std::mutex> lock(
	                write_queue_mutex_
	            );

	            write_queue_cv_.wait(
	                lock,
	                [this]() {
	                    return
	                        stopping_ ||
	                        !pending_writes_.empty();
	                }
	            );

	            /*
	             * During shutdown, only exit once all
	             * already-queued writes have been handled.
	             */
	            if (
	                stopping_ &&
	                pending_writes_.empty()
	            ) {
	                return;
	            }
	        }

	        processWriteBatches();
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

	void KeyValueStore::set(
	    const std::string& key,
	    const std::string& value
	)
	{
	    submitWrite(
	        WriteType::SET,
	        key,
	        value
	    );
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

	bool KeyValueStore::remove(
	    const std::string& key
	)
	{
	    return submitWrite(
	        WriteType::DELETE,
	        key,
	        ""
	    );
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

	bool KeyValueStore::submitWrite(
	    WriteType type,
	    const std::string& key,
	    const std::string& value
	)
	{
	    auto request =
	        std::make_shared<PendingWrite>();

	    request->type = type;
	    request->key = key;
	    request->value = value;

	    std::future<bool> result =
	        request->completion.get_future();

	    {
	        std::lock_guard<std::mutex> lock(
	            write_queue_mutex_
	        );

	        pending_writes_.push_back(request);
	    }

	    /*
	     * Wake the dedicated writer thread.
	     */
	    write_queue_cv_.notify_one();

	    /*
	     * This worker waits until the writer has:
	     *
	     * WAL write
	     *      ↓
	     * fsync
	     *      ↓
	     * MemTable update
	     */
	    return result.get();
	}

	void KeyValueStore::processWriteBatches()
	{
	    while (true) {

	        std::deque<std::shared_ptr<PendingWrite>> batch;

	        {
	            std::lock_guard<std::mutex> lock(
	                write_queue_mutex_
	            );

	            // No more work.
	            // We can safely give up leadership.
	            if (pending_writes_.empty()) {
	                return;
	            }

	            batch.swap(pending_writes_);
	        }

	        try {

	            /*
	             * Only the batch leader modifies the actual
	             * KeyValueStore write state.
	             *
	             * Readers are also prevented from observing
	             * a partially-applied batch.
	             */
	            std::unique_lock<std::shared_mutex> store_lock(
	                mutex_
	            );

	            /*
	             * Step 1:
	             * Write every operation in this batch to WAL.
	             *
	             * No fsync yet.
	             */
	            for (const auto& request : batch) {

	                if (request->type == WriteType::SET) {

	                    wal_.appendWithoutSync(
	                        "SET " +
	                        request->key +
	                        " " +
	                        request->value
	                    );
	                }
	                else {

	                    wal_.appendWithoutSync(
	                        "DELETE " +
	                        request->key
	                    );
	                }
	            }

	            /*
	             * Step 2:
	             * One fsync makes the entire batch durable.
	             */
	            wal_.sync();

	            /*
	             * Step 3:
	             * Apply operations to the MemTable
	             * in exactly the same order as WAL.
	             */
	            for (auto& request : batch) {

	                if (request->type == WriteType::SET) {

	                    memtable_.set(
	                        request->key,
	                        request->value
	                    );

	                    request->completion.set_value(true);
	                }
	                else {

	                    bool existed = false;

	                    if (memtable_.contains(request->key)) {

	                        existed = true;
	                    }
	                    else if (memtable_.isDeleted(request->key)) {

	                        existed = false;
	                    }
	                    else {

	                        auto tables = listSSTables();

	                        for (
	                            auto it = tables.rbegin();
	                            it != tables.rend();
	                            ++it
	                        ) {

	                            auto result =
	                                SSTable::lookup(
	                                    *it,
	                                    request->key
	                                );

	                            if (!result.has_value()) {
	                                continue;
	                            }

	                            if (result->deleted) {
	                                existed = false;
	                            }
	                            else {
	                                existed = true;
	                            }

	                            break;
	                        }
	                    }

	                    /*
	                     * WAL already contains this DELETE,
	                     * so apply its tombstone even if the
	                     * key did not previously exist.
	                     */
	                    memtable_.remove(request->key);

	                    request->completion.set_value(existed);
	                }
	            }

	            /*
	             * Step 4:
	             * Flush only after the complete durable batch
	             * has been applied.
	             */
	            if (
	                memtable_.shouldFlush(
	                    MEMTABLE_MAX_ENTRIES
	                )
	            ) {
	                flushMemTable();
	            }
	        }
	        catch (...) {

	            std::exception_ptr error =
	                std::current_exception();

	            /*
	             * Any promises that haven't already been
	             * completed receive the exception.
	             */
	            for (auto& request : batch) {
	                try {
	                    request->completion.set_exception(
	                        error
	                    );
	                }
	                catch (...) {
	                    // Promise may already have been completed.
	                }
	            }

	            /*
	             * Also fail requests that were queued while
	             * this batch was running.
	             */
	            std::deque<std::shared_ptr<PendingWrite>> waiting;
	            {
	                std::lock_guard<std::mutex> lock(
	                    write_queue_mutex_
	                );

	                waiting.swap(pending_writes_);
	            }

	            for (auto& request : waiting) {
	                try {
	                    request->completion.set_exception(
	                        error
	                    );
	                }
	                catch (...) {
	                }
	            }

	            return;
	        }
	    }
	}
}
