#include "process/MaintenanceJobManager.h"
#include "access/IndexFileUtil.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

using namespace dbms;
using namespace std::chrono_literals;

namespace {
std::string queuedRow(unsigned id, unsigned status = 0) {
    return std::to_string(id) + "\t1\t" + std::to_string(status) +
        "\t0\t6462\t\t\t\t0\t0\t\t1\t0\t0\t0\n";
}
void writeState(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    stream << bytes;
    if (!stream) throw std::runtime_error("fixture write failed");
}
std::string readState(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(stream), {}};
}
template<class Predicate> bool waitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return predicate();
}
}

int main() {
    char templatePath[] = "/tmp/dbms-maintenance-gate-XXXXXX";
    const char* created = ::mkdtemp(templatePath);
    if (!created) return 2;
    const std::filesystem::path root(created), state = root / "jobs.state";
    size_t checks = 0, failures = 0;
    const auto check = [&](bool good, const char* label) {
        ++checks;
        if (!good) { ++failures; std::cerr << "MAINTENANCE_GATE_FAILURE " << label << '\n'; }
    };

    for (const auto& bytes : {
             std::string("DBMS_MAINTENANCE_JOBS_V1\n3\n") + queuedRow(1) + "bad tail\n",
             std::string("DBMS_MAINTENANCE_JOBS_V1\n3\n") + queuedRow(1) + queuedRow(1),
             std::string("DBMS_MAINTENANCE_JOBS_V1\n-1\n") + queuedRow(1),
             std::string("DBMS_MAINTENANCE_JOBS_V1\n+3\n") + queuedRow(1),
             std::string("DBMS_MAINTENANCE_JOBS_V1\n3\n") + queuedRow(1) +
                 "18446744073709551615\t1\t0\t0\t6462\t\t\t\t0\t0\t\t1\t0\t0\t0\n"}) {
        writeState(state, bytes);
        std::atomic<unsigned> calls{0};
        {
            MaintenanceJobManager manager(state, [&](const auto&, const auto&, std::string&) {
                ++calls; return true;
            });
            check(!manager.persistenceHealthy(), "entire malformed file is rejected");
            check(manager.list().empty(), "no accepted prefix remains visible");
            std::string error;
            check(manager.submit({}, error) == 0 && !error.empty(), "bad state rejects new work");
            check(!manager.cancel(1, error), "bad state rejects cancellation writes");
            std::this_thread::sleep_for(100ms);
            check(calls == 0, "no malformed-file prefix reaches runner");
        }
        check(readState(state) == bytes, "bad state file remains unchanged");
    }

    // Constructor recovery must not dispatch when its Queued publication fails.
    writeState(state, "DBMS_MAINTENANCE_JOBS_V1\n2\n" + queuedRow(1, 1));
    std::atomic<unsigned> recoveryCalls{0};
    index_file::failNextDirectorySyncForTesting();
    {
        MaintenanceJobManager manager(state, [&](const auto&, const auto&, std::string&) {
            ++recoveryCalls; return true;
        });
        check(!manager.persistenceHealthy(), "failed startup publication marks state unavailable");
        std::this_thread::sleep_for(100ms);
        check(recoveryCalls == 0, "failed startup publication dispatches nothing");
    }

    for (bool failRunning : {true, false}) {
        writeState(state, "DBMS_MAINTENANCE_JOBS_V1\n1\n");
        std::mutex mutex;
        std::condition_variable condition;
        bool firstEntered = false, releaseFirst = false;
        std::atomic<unsigned> calls{0};
        {
            MaintenanceJobManager manager(state, [&](const auto& spec, const auto&, std::string&) {
                ++calls;
                if (spec.database == "first") {
                    std::unique_lock<std::mutex> lock(mutex);
                    firstEntered = true;
                    condition.notify_all();
                    condition.wait(lock, [&] { return releaseFirst; });
                }
                return true;
            });
            std::string error;
            MaintenanceJobSpec first, second;
            first.database = "first"; second.database = "second";
            const auto firstId = manager.submit(first, error);
            {
                std::unique_lock<std::mutex> lock(mutex);
                check(condition.wait_for(lock, 3s, [&] { return firstEntered; }), "first task enters runner");
            }
            const auto secondId = manager.submit(second, error);
            check(firstId != 0 && secondId != 0, "both queued publications succeed");
            // After release: terminal(first), then Running(second), in order.
            if (failRunning) index_file::failDirectorySyncAfterForTesting(1);
            else index_file::failNextDirectorySyncForTesting();
            {
                std::lock_guard<std::mutex> lock(mutex);
                releaseFirst = true;
            }
            condition.notify_all();
            check(waitUntil([&] { return !manager.persistenceHealthy(); }), "injected publication failure is observed");
            std::this_thread::sleep_for(100ms);
            check(calls == 1, "no runner dispatch after publication failure");
            const auto secondJob = manager.find(secondId);
            check(secondJob && secondJob->status == MaintenanceJobStatus::Queued &&
                  secondJob->startedAt == 0, "unstarted task retains its Queued image");
            check(manager.submit({}, error) == 0 && !error.empty(), "failed publication rejects further submissions");
            check(!manager.cancel(secondId, error), "failed publication rejects cancellation writes");
        }
        index_file::directorySyncFailureCountForTesting.store(0);
    }

    // Repair/restart is explicit; there is no blind in-process retry of work.
    writeState(state, "DBMS_MAINTENANCE_JOBS_V1\n2\n" + queuedRow(1, 1));
    std::atomic<unsigned> normalCalls{0};
    {
        MaintenanceJobManager manager(state, [&](const auto&, const auto&, std::string&) {
            ++normalCalls; return true;
        });
        check(manager.persistenceHealthy(), "normal recovery state is healthy");
        check(waitUntil([&] {
            const auto job = manager.find(1);
            return job && job->status == MaintenanceJobStatus::Succeeded;
        }), "Running image resumes after healthy publication");
        check(normalCalls == 1, "normal recovery executes exactly once");
    }
    std::filesystem::remove_all(root);
    std::cout << "MAINTENANCE_GATE_CHECKS " << checks << " FAILURES " << failures << std::endl;
    return failures ? 1 : 0;
}
