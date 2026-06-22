#pragma once

#include "catalog/catalog.h"
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace dbms {

class StorageEngine;

// ============================================================================
// CatalogService: per-database CatalogManager cache
// ============================================================================
//
// CatalogManager is per-database (constructed with a dbPath). This service
// lazily creates one CatalogManager per database on first access, bootstraps
// system namespaces/types, and optionally migrates existing .stc-based
// databases on first use. It persists all cached catalogs on shutdown or
// explicit persistAll().
//
// Thread-safety: the cache map is protected by its own mutex; each
// CatalogManager is internally mutex-guarded. Lock ordering is always
// CatalogService::mutex_ -> CatalogManager::mutex_.
//
class CatalogService {
public:
    explicit CatalogService(const StorageEngine& engine);
    ~CatalogService();

    CatalogService(const CatalogService&) = delete;
    CatalogService& operator=(const CatalogService&) = delete;

    // Get or create the CatalogManager for `dbname`.
    // On first creation: bootstraps system namespaces/types and runs a
    // one-time migration for pre-existing .stc databases.
    CatalogManager& get(const std::string& dbname);

    // Remove the cached CatalogManager for `dbname` (e.g. on DROP DATABASE).
    // The CatalogManager destructor persists its state.
    void evict(const std::string& dbname);

    // Persist all cached catalogs.
    void persistAll();

    // Test/debug: is `dbname` currently cached?
    bool has(const std::string& dbname) const;

    // Convert a physical storage name (schema__table or plain_table) to a
    // logical qualified name. This is the inverse of main.cpp's
    // resolveTableName() for schema-qualified names.
    static CatalogManager::QualifiedName logicalName(const std::string& physical);

private:
    const StorageEngine& engine_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<CatalogManager>> cache_;
};

} // namespace dbms
