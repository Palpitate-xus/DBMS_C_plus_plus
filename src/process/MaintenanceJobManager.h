#pragma once

#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace dbms {

enum class MaintenanceJobKind {
    Dump,
    Backup,
    Restore,
    PitrRestore,
    ClearPlanCache,
};

enum class MaintenanceJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
};

struct MaintenanceJobSpec {
    MaintenanceJobKind kind = MaintenanceJobKind::Dump;
    std::string database;
    std::string path;
    std::string archivePath;
    uint64_t targetEpoch = 0;
    uint64_t rateLimitKiB = 0;
    std::string requestedBy;
};

struct MaintenanceJobSnapshot {
    uint64_t id = 0;
    MaintenanceJobSpec spec;
    MaintenanceJobStatus status = MaintenanceJobStatus::Queued;
    bool cancelRequested = false;
    std::string error;
    uint64_t submittedAt = 0;
    uint64_t startedAt = 0;
    uint64_t finishedAt = 0;
};

class MaintenanceJobControl {
public:
    bool cancelled() const;
    bool accountBytes(uint64_t bytes) const;

private:
    friend class MaintenanceJobManager;
    std::function<bool()> cancelled_;
    std::function<bool(uint64_t)> accountBytes_;
};

class MaintenanceJobManager {
public:
    using Runner = std::function<bool(
        const MaintenanceJobSpec&, const MaintenanceJobControl&,
        std::string&)>;

    MaintenanceJobManager(std::filesystem::path statePath, Runner runner);
    ~MaintenanceJobManager();

    MaintenanceJobManager(const MaintenanceJobManager&) = delete;
    MaintenanceJobManager& operator=(const MaintenanceJobManager&) = delete;

    uint64_t submit(const MaintenanceJobSpec& spec, std::string& error);
    bool cancel(uint64_t id, std::string& error);
    std::optional<MaintenanceJobSnapshot> find(uint64_t id) const;
    std::vector<MaintenanceJobSnapshot> list() const;
    bool persistenceHealthy() const;

    static const char* kindName(MaintenanceJobKind kind);
    static const char* statusName(MaintenanceJobStatus status);

private:
    void workerLoop();
    bool isCancellationRequested(uint64_t id) const;
    bool accountBytes(uint64_t id, uint64_t bytes,
                      std::chrono::steady_clock::time_point started);
    bool loadLocked();
    bool persistLocked();

    std::filesystem::path statePath_;
    Runner runner_;
    mutable std::mutex mutex_;
    std::condition_variable workAvailable_;
    std::map<uint64_t, MaintenanceJobSnapshot> jobs_;
    uint64_t nextId_ = 1;
    bool stopping_ = false;
    bool persistenceHealthy_ = true;
    std::thread worker_;
};

}  // namespace dbms
