#pragma once

#include <string>
#include <vector>
#include <thread>
#include <mutex>

namespace dbms {

// Process backend types for Phase 9.5
enum class BackendType {
    ClientBackend,     // handles one client connection
    WalWriter,         // background WAL writer
    BgWriter,          // background buffer writer
    Checkpointer,      // periodic checkpoint
    AutoVacuumLauncher,// autovacuum worker launcher
    AutoVacuumWorker,  // autovacuum worker
    ReplicationSender, // streaming replication sender
    Archiver,          // WAL archiver
    StatsCollector,    // pg_stat collector
    LogicalLauncher,   // logical replication launcher
    LogicalWorker,     // logical replication worker
};

struct BackendInfo {
    BackendType type;
    int pid = 0;
    std::string dbname;
    std::string username;
    bool running = false;
    std::thread::id threadId;
};

// Manages the multi-process (multi-thread for now) backend pool.
// In a full PG implementation this uses fork()/shared memory;
// here we use threads as the execution model.
class ProcessManager {
public:
    static ProcessManager& instance();

    // Spawn a background worker thread.
    bool spawnBackend(BackendType type, const std::string& dbname = "",
                       const std::string& username = "");

    // Gracefully stop all backends.
    void shutdown();

    // List active backends.
    std::vector<BackendInfo> listBackends() const;

    // Active count.
    size_t activeBackends() const;

private:
    ProcessManager() = default;
    mutable std::mutex mutex_;
    std::vector<BackendInfo> backends_;
    bool shutdown_ = false;
};

} // namespace dbms
