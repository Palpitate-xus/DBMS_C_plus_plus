#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "catalog/migrate.h"
#include <filesystem>
#include <fstream>

namespace dbms {

namespace fs = std::filesystem;

CatalogService::CatalogService(const StorageEngine& engine) : engine_(engine) {}

CatalogService::~CatalogService() {
    persistAll();
}

static fs::path catalogDirForDb(const StorageEngine& engine, const std::string& dbname) {
    return engine.dbPath(dbname) / "pg_catalog";
}

static fs::path markerPath(const StorageEngine& engine, const std::string& dbname) {
    return catalogDirForDb(engine, dbname) / ".migrated";
}

static bool hasStcFiles(const fs::path& dbPath) {
    if (!fs::exists(dbPath) || !fs::is_directory(dbPath)) return false;
    for (const auto& entry : fs::directory_iterator(dbPath)) {
        if (!entry.is_regular_file()) continue;
        const std::string name = entry.path().filename().string();
        if (name.size() > 4 && name.substr(name.size() - 4) == ".stc") {
            return true;
        }
    }
    return false;
}

CatalogManager& CatalogService::get(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(dbname);
    if (it != cache_.end()) {
        return *it->second;
    }

    auto cat = std::make_unique<CatalogManager>(catalogDirForDb(engine_, dbname).string());
    cat->bootstrapSystemNamespaces();
    cat->bootstrapSystemTypes();

    // One-time migration for databases created before the catalog existed.
    fs::path marker = markerPath(engine_, dbname);
    if (!fs::exists(marker)) {
        fs::path dbPath = engine_.dbPath(dbname);
        if (hasStcFiles(dbPath)) {
            MigrateResult res = migrateDatabaseToCatalog(*cat, engine_, dbname);
            if (res.ok()) {
                // Persist immediately after successful migration.
                cat->persistAll();
                std::ofstream(marker, std::ios::out | std::ios::trunc).close();
            }
            // If migration fails, we leave the marker absent so a future get()
            // can retry. Errors are logged by migrateDatabaseToCatalog.
        } else {
            // No .stc files means the database is new/empty; mark as migrated
            // to avoid re-scanning on every get().
            std::ofstream(marker, std::ios::out | std::ios::trunc).close();
        }
    }

    CatalogManager& ref = *cat;
    cache_.emplace(dbname, std::move(cat));
    return ref;
}

void CatalogService::evict(const std::string& dbname) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(dbname);
}

void CatalogService::persistAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& kv : cache_) {
        if (kv.second) kv.second->persistAll();
    }
}

bool CatalogService::has(const std::string& dbname) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_.find(dbname) != cache_.end();
}

} // namespace dbms
