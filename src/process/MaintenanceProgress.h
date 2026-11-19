#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace dbms {

enum class MaintenanceCommand { Vacuum, Analyze, VacuumFull };
using MaintenanceCounters = std::array<uint64_t, 10>;

struct MaintenanceProgressRow {
    MaintenanceCommand command;
    int32_t pid;
    std::string database;
    std::string user;
    uint32_t relation;
    const char* phase;
    MaintenanceCounters counters{};
};

// One stack-owned entry per active backend command. An internal maintenance
// call may briefly take over the backend entry, then restore its caller's.
class ScopedMaintenanceProgress {
public:
    ScopedMaintenanceProgress(MaintenanceCommand command, uint64_t pid,
        const std::string& database, const std::string& user, uint32_t relation) noexcept;
    ~ScopedMaintenanceProgress();
    ScopedMaintenanceProgress(const ScopedMaintenanceProgress&) = delete;
    ScopedMaintenanceProgress& operator=(const ScopedMaintenanceProgress&) = delete;

    explicit operator bool() const { return row_.pid != 0; }
    // Phases are static names. Publish phase and associated counters together.
    void publish(const char* phase, const MaintenanceCounters& counters = {});
    void setPhase(const char* phase);
    void addVacuumPages(uint64_t scanned, uint64_t compacted);

private:
    MaintenanceProgressRow row_;
    MaintenanceProgressRow* previous_ = nullptr;
};

std::vector<MaintenanceProgressRow> getMaintenanceProgress(MaintenanceCommand command);

} // namespace dbms
