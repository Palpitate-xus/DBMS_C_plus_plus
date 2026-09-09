#include "process/MaintenanceJobManager.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

using namespace dbms;

namespace {

bool waitForStatus(MaintenanceJobManager& manager, uint64_t id,
                   MaintenanceJobStatus expected,
                   std::chrono::milliseconds timeout =
                       std::chrono::milliseconds(3000)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        const auto job = manager.find(id);
        if (job && job->status == expected) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

}  // namespace

int main() {
    const auto root = std::filesystem::path("maintenance_job_test");
    const auto state = root / "jobs.state";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);

    {
        MaintenanceJobManager manager(
            state,
            [](const MaintenanceJobSpec& spec,
               const MaintenanceJobControl& control,
               std::string& error) {
                if (spec.database == "fail") {
                    error = "injected failure";
                    return false;
                }
                if (spec.database == "wait") {
                    while (!control.cancelled()) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(5));
                    }
                    return false;
                }
                return control.accountBytes(10 * 1024);
            });
        assert(manager.persistenceHealthy());

        std::string error;
        MaintenanceJobSpec rateLimited;
        rateLimited.kind = MaintenanceJobKind::Backup;
        rateLimited.database = "ok";
        rateLimited.path = "path with spaces";
        rateLimited.requestedBy = "admin";
        rateLimited.rateLimitKiB = 100;
        const auto started = std::chrono::steady_clock::now();
        const uint64_t successId = manager.submit(rateLimited, error);
        assert(successId != 0 && error.empty());
        assert(waitForStatus(
            manager, successId, MaintenanceJobStatus::Succeeded));
        assert(std::chrono::steady_clock::now() - started >=
               std::chrono::milliseconds(80));
        const auto success = manager.find(successId);
        assert(success && success->spec.path == "path with spaces");

        MaintenanceJobSpec failing;
        failing.database = "fail";
        const uint64_t failureId = manager.submit(failing, error);
        assert(waitForStatus(
            manager, failureId, MaintenanceJobStatus::Failed));
        assert(manager.find(failureId)->error == "injected failure");

        MaintenanceJobSpec waiting;
        waiting.database = "wait";
        const uint64_t cancelId = manager.submit(waiting, error);
        assert(waitForStatus(
            manager, cancelId, MaintenanceJobStatus::Running));
        assert(manager.cancel(cancelId, error));
        assert(waitForStatus(
            manager, cancelId, MaintenanceJobStatus::Cancelled));
        assert(manager.list().size() == 3);
    }

    // A job recorded as running was interrupted by a process crash. A new
    // manager must put it back in the queue and execute it exactly as a job,
    // never report the stale running state forever.
    {
        std::ofstream fixture(state, std::ios::trunc);
        fixture << "DBMS_MAINTENANCE_JOBS_V1\n"
                << "8\n"
                << "7\t1\t1\t0\t6462\t2f746d702f62\t\t61646d696e"
                   "\t0\t0\t\t1\t1\t0\t0\n";
    }
    {
        MaintenanceJobManager resumed(
            state,
            [](const MaintenanceJobSpec& spec,
               const MaintenanceJobControl&, std::string&) {
                return spec.database == "db" && spec.path == "/tmp/b";
            });
        assert(waitForStatus(
            resumed, 7, MaintenanceJobStatus::Succeeded));
        std::string error;
        MaintenanceJobSpec next;
        next.database = "db";
        next.path = "/tmp/b";
        assert(resumed.submit(next, error) == 8);
        assert(waitForStatus(
            resumed, 8, MaintenanceJobStatus::Succeeded));
    }

    std::filesystem::remove_all(root);
    return 0;
}
