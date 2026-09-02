// TOAST chunked relation test.

#include "TableManage.h"
#include "Config.h"
#include <atomic>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <cassert>
#include <thread>
#include <vector>

dbms::Config g_config;

using namespace dbms;

int main() {
    std::string dbname = "toast_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.formatVersion = 2;
        tbl.append(makeVarCharColumn("payload", false, 10000, false));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);

        // Verify TOAST relation and index files were created.
        assert(std::filesystem::exists(std::filesystem::path(dbname) / "t.toast.dt"));
        assert(std::filesystem::exists(std::filesystem::path(dbname) / "t.toast.idx"));

        const auto metaPath = std::filesystem::path(dbname) / "t.toastmeta";
        assert(std::filesystem::file_size(metaPath) == 24);
        std::ifstream metaIn(metaPath, std::ios::binary);
        const std::string validMeta((std::istreambuf_iterator<char>(metaIn)),
                                    std::istreambuf_iterator<char>());
        assert(validMeta.size() == 24);

        // A truncated or checksum-corrupt allocator state must not silently
        // restart at ID 1 and overwrite an existing external value.
        {
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out.write(validMeta.data(), 7);
        }
        assert(engine.insert(dbname, "t", {{"payload", std::string(10000, 'x')}})
               == DBStatus::IO_ERROR);
        {
            std::string corrupt = validMeta;
            corrupt.back() ^= 1;
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out.write(corrupt.data(), static_cast<std::streamsize>(corrupt.size()));
        }
        assert(engine.insert(dbname, "t", {{"payload", std::string(10000, 'y')}})
               == DBStatus::IO_ERROR);
        {
            std::ofstream out(metaPath, std::ios::binary | std::ios::trunc);
            out.write(validMeta.data(), static_cast<std::streamsize>(validMeta.size()));
        }
        assert(engine.query(dbname, "t", {}, {"payload"}).empty());
        std::cout << "[TOAST] corrupt ID allocator fails closed OK\n";

        std::string largeValue(10000, 'a');

        // Insert a row with a large value that exceeds the TOAST threshold.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> vals;
        vals["payload"] = largeValue;
        assert(engine.insert(dbname, "t", vals) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        // Query should return the original large value.
        auto rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(largeValue) != std::string::npos);
        // The current TOAST format compresses repetitive values before
        // chunking.  Account for the fixed 8 KiB relation header page: the
        // compressed payload should fit in one additional data page, while
        // an uncompressed 10 KiB value would require multiple chunks/pages.
        constexpr uintmax_t toastPageSize = 8192;
        assert(std::filesystem::file_size(std::filesystem::path(dbname) / "t.toast.dt") <=
               toastPageSize * 2);
        std::cout << "[TOAST] insert + query large value OK\n";

        // Update with another large value.
        std::string updatedValue(12000, 'b');
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        std::map<std::string, std::string> newVals;
        newVals["payload"] = updatedValue;
        assert(engine.update(dbname, "t", newVals, {}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.size() == 1);
        assert(rows[0].find(updatedValue) != std::string::npos);
        assert(rows[0].find(largeValue) == std::string::npos);
        std::cout << "[TOAST] update large value OK\n";

        // Delete the row.
        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.remove(dbname, "t", {}) == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);

        rows = engine.query(dbname, "t", {}, {"payload"});
        assert(rows.empty());
        std::cout << "[TOAST] delete large value OK\n";

        TableSchema concurrent;
        concurrent.tablename = "tc";
        concurrent.formatVersion = 2;
        concurrent.append(makeVarCharColumn("payload", false, 12000, false));
        assert(engine.createTable(dbname, concurrent) == DBStatus::OK);

        constexpr int workerCount = 12;
        std::atomic<int> ready{0};
        std::atomic<bool> start{false};
        std::atomic<bool> failed{false};
        std::vector<std::thread> workers;
        for (int worker = 0; worker < workerCount; ++worker) {
            workers.emplace_back([&, worker] {
                std::string payload(10000, '\0');
                for (size_t i = 0; i < payload.size(); ++i)
                    payload[i] = static_cast<char>('!' + ((i * 17 + worker * 29) % 90));
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
                if (engine.insert(dbname, "tc", {{"payload", payload}}) != DBStatus::OK)
                    failed.store(true, std::memory_order_release);
            });
        }
        while (ready.load(std::memory_order_acquire) != workerCount)
            std::this_thread::yield();
        start.store(true, std::memory_order_release);
        for (auto& worker : workers) worker.join();
        assert(!failed.load(std::memory_order_acquire));
        assert(engine.query(dbname, "tc", {}, {"payload"}).size() == workerCount);
        std::cout << "[TOAST] concurrent ID allocation unique OK\n";
    }

    // Cleanup
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    std::cout << "[TOAST] all passed\n";
    return 0;
}
