#include "MaintenanceJobManager.h"

#include "access/IndexFileUtil.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace dbms {
namespace {

uint64_t epochSeconds() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

std::string hexEncode(const std::string& input) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string output;
    output.reserve(input.size() * 2);
    for (unsigned char byte : input) {
        output.push_back(digits[byte >> 4]);
        output.push_back(digits[byte & 0x0f]);
    }
    return output;
}

bool hexDecode(const std::string& input, std::string& output) {
    if (input.size() % 2 != 0) return false;
    const auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    output.clear();
    output.reserve(input.size() / 2);
    for (size_t i = 0; i < input.size(); i += 2) {
        const int high = nibble(input[i]);
        const int low = nibble(input[i + 1]);
        if (high < 0 || low < 0) return false;
        output.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

bool parseUnsigned(const std::string& text, uint64_t& value) {
    try {
        size_t consumed = 0;
        const auto parsed = std::stoull(text, &consumed);
        if (consumed != text.size()) return false;
        value = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<std::string> splitTabs(const std::string& line) {
    std::vector<std::string> fields;
    size_t start = 0;
    while (true) {
        const size_t tab = line.find('\t', start);
        fields.push_back(line.substr(start, tab - start));
        if (tab == std::string::npos) break;
        start = tab + 1;
    }
    return fields;
}

bool decodeKind(uint64_t raw, MaintenanceJobKind& kind) {
    if (raw > static_cast<uint64_t>(MaintenanceJobKind::ClearPlanCache))
        return false;
    kind = static_cast<MaintenanceJobKind>(raw);
    return true;
}

bool decodeStatus(uint64_t raw, MaintenanceJobStatus& status) {
    if (raw > static_cast<uint64_t>(MaintenanceJobStatus::Cancelled))
        return false;
    status = static_cast<MaintenanceJobStatus>(raw);
    return true;
}

}  // namespace

bool MaintenanceJobControl::cancelled() const {
    return cancelled_ && cancelled_();
}

bool MaintenanceJobControl::accountBytes(uint64_t bytes) const {
    return accountBytes_ ? accountBytes_(bytes) : !cancelled();
}

const char* MaintenanceJobManager::kindName(MaintenanceJobKind kind) {
    switch (kind) {
        case MaintenanceJobKind::Dump: return "dump";
        case MaintenanceJobKind::Backup: return "backup";
        case MaintenanceJobKind::Restore: return "restore";
        case MaintenanceJobKind::PitrRestore: return "pitr_restore";
        case MaintenanceJobKind::ClearPlanCache: return "clear_plan_cache";
    }
    return "unknown";
}

const char* MaintenanceJobManager::statusName(MaintenanceJobStatus status) {
    switch (status) {
        case MaintenanceJobStatus::Queued: return "queued";
        case MaintenanceJobStatus::Running: return "running";
        case MaintenanceJobStatus::Succeeded: return "succeeded";
        case MaintenanceJobStatus::Failed: return "failed";
        case MaintenanceJobStatus::Cancelled: return "cancelled";
    }
    return "unknown";
}

MaintenanceJobManager::MaintenanceJobManager(
    std::filesystem::path statePath, Runner runner)
    : statePath_(std::move(statePath)), runner_(std::move(runner)) {
    if (!runner_) throw std::invalid_argument("maintenance job runner is required");
    {
        std::lock_guard<std::mutex> lock(mutex_);
        persistenceHealthy_ = loadLocked();
        if (persistenceHealthy_) {
            // A process crash leaves no live worker. Running jobs are safe to
            // resume because each operation publishes through staging paths.
            for (auto& [id, job] : jobs_) {
                (void)id;
                if (job.status == MaintenanceJobStatus::Running) {
                    job.status = MaintenanceJobStatus::Queued;
                    job.startedAt = 0;
                    job.finishedAt = 0;
                    job.error.clear();
                }
            }
            persistenceHealthy_ = persistLocked();
        }
    }
    worker_ = std::thread(&MaintenanceJobManager::workerLoop, this);
}

MaintenanceJobManager::~MaintenanceJobManager() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    workAvailable_.notify_all();
    if (worker_.joinable()) worker_.join();
}

uint64_t MaintenanceJobManager::submit(
    const MaintenanceJobSpec& spec, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!persistenceHealthy_) {
        error = "maintenance job state is unavailable";
        return 0;
    }
    MaintenanceJobSnapshot job;
    job.id = nextId_++;
    job.spec = spec;
    job.submittedAt = epochSeconds();
    jobs_.emplace(job.id, job);
    if (!persistLocked()) {
        jobs_.erase(job.id);
        --nextId_;
        persistenceHealthy_ = false;
        error = "could not persist maintenance job";
        return 0;
    }
    workAvailable_.notify_one();
    return job.id;
}

bool MaintenanceJobManager::cancel(uint64_t id, std::string& error) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = jobs_.find(id);
    if (found == jobs_.end()) {
        error = "maintenance job does not exist";
        return false;
    }
    auto& job = found->second;
    if (job.status == MaintenanceJobStatus::Succeeded ||
        job.status == MaintenanceJobStatus::Failed ||
        job.status == MaintenanceJobStatus::Cancelled) {
        error = "maintenance job has already finished";
        return false;
    }
    job.cancelRequested = true;
    if (job.status == MaintenanceJobStatus::Queued) {
        job.status = MaintenanceJobStatus::Cancelled;
        job.finishedAt = epochSeconds();
    }
    if (!persistLocked()) {
        persistenceHealthy_ = false;
        error = "could not persist maintenance job cancellation";
        return false;
    }
    workAvailable_.notify_all();
    return true;
}

std::optional<MaintenanceJobSnapshot> MaintenanceJobManager::find(
    uint64_t id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = jobs_.find(id);
    if (found == jobs_.end()) return std::nullopt;
    return found->second;
}

std::vector<MaintenanceJobSnapshot> MaintenanceJobManager::list() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<MaintenanceJobSnapshot> result;
    result.reserve(jobs_.size());
    for (const auto& [id, job] : jobs_) {
        (void)id;
        result.push_back(job);
    }
    return result;
}

bool MaintenanceJobManager::persistenceHealthy() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return persistenceHealthy_;
}

bool MaintenanceJobManager::isCancellationRequested(uint64_t id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = jobs_.find(id);
    return stopping_ || found == jobs_.end() || found->second.cancelRequested;
}

bool MaintenanceJobManager::accountBytes(
    uint64_t id, uint64_t bytes,
    std::chrono::steady_clock::time_point started) {
    uint64_t rate = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = jobs_.find(id);
        if (stopping_ || found == jobs_.end() ||
            found->second.cancelRequested) return false;
        rate = found->second.spec.rateLimitKiB;
    }
    if (rate == 0 || bytes == 0) return true;

    const auto minimum = std::chrono::duration<double>(
        static_cast<double>(bytes) /
        (static_cast<double>(rate) * 1024.0));
    while (std::chrono::steady_clock::now() - started < minimum) {
        if (isCancellationRequested(id)) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return !isCancellationRequested(id);
}

void MaintenanceJobManager::workerLoop() {
    while (true) {
        uint64_t id = 0;
        MaintenanceJobSpec spec;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            workAvailable_.wait(lock, [&] {
                if (stopping_) return true;
                return std::any_of(jobs_.begin(), jobs_.end(), [](const auto& item) {
                    return item.second.status == MaintenanceJobStatus::Queued;
                });
            });
            if (stopping_) return;
            for (auto& [candidateId, job] : jobs_) {
                if (job.status != MaintenanceJobStatus::Queued) continue;
                id = candidateId;
                job.status = MaintenanceJobStatus::Running;
                job.startedAt = epochSeconds();
                spec = job.spec;
                if (!persistLocked()) persistenceHealthy_ = false;
                break;
            }
        }
        if (id == 0) continue;

        const auto started = std::chrono::steady_clock::now();
        uint64_t accountedBytes = 0;
        MaintenanceJobControl control;
        control.cancelled_ = [this, id] {
            return isCancellationRequested(id);
        };
        control.accountBytes_ =
            [this, id, started, &accountedBytes](uint64_t bytes) {
            if (UINT64_MAX - accountedBytes < bytes)
                accountedBytes = UINT64_MAX;
            else
                accountedBytes += bytes;
            return accountBytes(id, accountedBytes, started);
        };
        bool succeeded = false;
        std::string error;
        try {
            succeeded = runner_(spec, control, error);
        } catch (const std::exception& exception) {
            error = exception.what();
        } catch (...) {
            error = "unknown maintenance job failure";
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = jobs_.find(id);
            if (found == jobs_.end()) continue;
            auto& job = found->second;
            if (job.cancelRequested) {
                job.status = MaintenanceJobStatus::Cancelled;
                job.error.clear();
            } else if (succeeded) {
                job.status = MaintenanceJobStatus::Succeeded;
                job.error.clear();
            } else {
                job.status = MaintenanceJobStatus::Failed;
                job.error = error.empty() ? "maintenance operation failed" : error;
            }
            job.finishedAt = epochSeconds();
            if (!persistLocked()) persistenceHealthy_ = false;
        }
    }
}

bool MaintenanceJobManager::loadLocked() {
    jobs_.clear();
    nextId_ = 1;
    if (!std::filesystem::exists(statePath_)) return true;
    std::ifstream input(statePath_);
    if (!input) return false;
    std::string line;
    if (!std::getline(input, line) || line != "DBMS_MAINTENANCE_JOBS_V1")
        return false;
    if (!std::getline(input, line) || !parseUnsigned(line, nextId_) ||
        nextId_ == 0) return false;
    while (std::getline(input, line)) {
        if (line.empty()) continue;
        const auto fields = splitTabs(line);
        if (fields.size() != 15) return false;
        MaintenanceJobSnapshot job;
        uint64_t kind = 0, status = 0, cancelled = 0;
        if (!parseUnsigned(fields[0], job.id) || job.id == 0 ||
            !parseUnsigned(fields[1], kind) || !decodeKind(kind, job.spec.kind) ||
            !parseUnsigned(fields[2], status) || !decodeStatus(status, job.status) ||
            !parseUnsigned(fields[3], cancelled) || cancelled > 1 ||
            !parseUnsigned(fields[8], job.spec.targetEpoch) ||
            !parseUnsigned(fields[9], job.spec.rateLimitKiB) ||
            !parseUnsigned(fields[11], job.submittedAt) ||
            !parseUnsigned(fields[12], job.startedAt) ||
            !parseUnsigned(fields[13], job.finishedAt) ||
            !hexDecode(fields[4], job.spec.database) ||
            !hexDecode(fields[5], job.spec.path) ||
            !hexDecode(fields[6], job.spec.archivePath) ||
            !hexDecode(fields[7], job.spec.requestedBy) ||
            !hexDecode(fields[10], job.error)) return false;
        uint64_t reserved = 0;
        if (!parseUnsigned(fields[14], reserved) || reserved != 0) return false;
        job.cancelRequested = cancelled != 0;
        if (!jobs_.emplace(job.id, std::move(job)).second) return false;
        nextId_ = std::max(nextId_, job.id + 1);
    }
    return input.eof();
}

bool MaintenanceJobManager::persistLocked() {
    std::ostringstream output;
    output << "DBMS_MAINTENANCE_JOBS_V1\n" << nextId_ << '\n';
    for (const auto& [id, job] : jobs_) {
        output << id << '\t'
               << static_cast<unsigned>(job.spec.kind) << '\t'
               << static_cast<unsigned>(job.status) << '\t'
               << (job.cancelRequested ? 1 : 0) << '\t'
               << hexEncode(job.spec.database) << '\t'
               << hexEncode(job.spec.path) << '\t'
               << hexEncode(job.spec.archivePath) << '\t'
               << hexEncode(job.spec.requestedBy) << '\t'
               << job.spec.targetEpoch << '\t'
               << job.spec.rateLimitKiB << '\t'
               << hexEncode(job.error) << '\t'
               << job.submittedAt << '\t' << job.startedAt << '\t'
               << job.finishedAt << "\t0\n";
    }
    const auto parent = statePath_.parent_path();
    std::error_code error;
    if (!parent.empty()) std::filesystem::create_directories(parent, error);
    if (error) return false;
    return index_file::writeAtomically(statePath_, output.str());
}

}  // namespace dbms
