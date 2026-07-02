#include "ProcessManager.h"

namespace dbms {

ProcessManager& ProcessManager::instance() {
    static ProcessManager mgr;
    return mgr;
}

bool ProcessManager::spawnBackend(BackendType type, const std::string& dbname,
                                    const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutdown_) return false;
    BackendInfo info;
    info.type = type;
    info.dbname = dbname;
    info.username = username;
    info.pid = static_cast<int>(backends_.size() + 1);
    info.running = true;
    backends_.push_back(std::move(info));
    return true;
}

void ProcessManager::shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
    for (auto& b : backends_) {
        b.running = false;
    }
    backends_.clear();
}

std::vector<BackendInfo> ProcessManager::listBackends() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return backends_;
}

size_t ProcessManager::activeBackends() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (const auto& b : backends_) {
        if (b.running) ++count;
    }
    return count;
}

} // namespace dbms
