#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include <filesystem>

namespace dbms {

namespace fs = std::filesystem;

CatalogService::CatalogService(const StorageEngine& engine) : engine_(engine) {}

CatalogService::~CatalogService() {
    persistAll();
}

static fs::path catalogDirForDb(const StorageEngine& engine, const std::string& dbname) {
    return engine.dbPath(dbname) / "pg_catalog";
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

CatalogManager::QualifiedName CatalogService::logicalName(const std::string& physical) {
    CatalogManager::QualifiedName qn;
    qn.schema.clear();
    qn.name = physical;

    size_t pos = physical.find("__");
    if (pos != std::string::npos && pos > 0 && pos + 2 < physical.size()) {
        qn.schema = physical.substr(0, pos);
        qn.name = physical.substr(pos + 2);
    }
    return qn;
}

} // namespace dbms
